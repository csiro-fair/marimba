"""
Marimba Packaging Checkpoint State.

Packaging a large dataset runs for many hours, so a run killed part-way (a time limit, a lost machine, an operator
interrupt) must be able to finish without redoing completed work. This module persists that progress under
``<project>/.marimba/package/<dataset>/``:

- ``run.json``: the fingerprint of the run's inputs, a digest of its destination paths, its start time and the status
  of each packaging stage. Rewritten atomically after every stage.
- ``hashes.jsonl``: an append-only journal of SHA-256 hashes computed while packaging, keyed by dataset-relative path
  and guarded by file size and modification time, so a resumed run never rehashes an unchanged file.
- ``verified.jsonl``: the same journal for the final validation pass, kept separate so validation always reads every
  byte at least once rather than trusting hashes taken earlier in the run.
- ``metadata_files.jsonl``: the files whose metadata has been embedded, with any state the embedding derived from the
  file (for example iFDO image entropy), so a resumed run can restore it without reprocessing the file.
- ``lock``: held by the running process and refreshed by a heartbeat, so a second run is refused while the first is
  alive but a lock left behind by a killed run is taken over.

JSON Lines is used rather than SQLite because SQLite locking is unreliable on the NFS and CIFS shares that large
packaging runs usually live on, and an append-only journal survives a hard kill with at most a torn final line.
"""

import contextlib
import hashlib
import json
import os
import shutil
import socket
import threading
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import IntEnum
from pathlib import Path
from typing import Any

from marimba.core.utils.hash import compute_hash
from marimba.core.utils.log import get_logger

logger = get_logger(__name__)

STATE_FORMAT_VERSION = 1


class PackageStage(IntEnum):
    """Packaging stages, numbered as they appear in the packaging progress bars."""

    COMPOSE = 1
    CHECK_MAPPING = 2
    POPULATE = 3
    PROCESS_METADATA_FILES = 4
    DATASET_METADATA = 5
    SUMMARY = 6
    MAP = 7
    PIPELINES = 8
    LOGS = 9
    MANIFEST = 10
    POST_PACKAGE = 11
    VALIDATE = 12

    @property
    def label(self) -> str:
        """Human-readable stage name."""
        return self.name.replace("_", " ").lower()


# Stages that always run, because they rebuild the in-memory dataset mapping and metadata later stages need.
ALWAYS_RUN_STAGES = frozenset({PackageStage.COMPOSE, PackageStage.CHECK_MAPPING})
FIRST_RESUMABLE_STAGE = PackageStage.POPULATE


class PackageStateError(Exception):
    """Raised when packaging checkpoint state cannot be created, read or locked."""


def destinations_digest(destinations: Iterable[str]) -> dict[str, Any]:
    """
    Summarise a run's destination paths as a count and an order-independent SHA-256 digest.

    Args:
        destinations: Dataset-relative destination path strings.

    Returns:
        A dictionary with ``count`` and ``sha256`` keys.
    """
    ordered = sorted(destinations)
    digest = hashlib.sha256()
    for destination in ordered:
        digest.update(destination.encode())
        digest.update(b"\0")
    return {"count": len(ordered), "sha256": digest.hexdigest()}


def mapping_destinations_digest(dataset_mapping: dict[str, dict[str, dict[Any, tuple[Any, ...]]]]) -> dict[str, Any]:
    """
    Digest the destination paths of a composed dataset mapping (pipeline -> collection -> source -> entry).

    Args:
        dataset_mapping: The mapping returned by ``ProjectWrapper.compose``, whose entries start with the
            pipeline-relative destination path.

    Returns:
        The digest from :func:`destinations_digest`, over ``<pipeline>/<destination>`` strings.
    """
    return destinations_digest(
        f"{pipeline}/{Path(entry[0]).as_posix()}"
        for pipeline, collections in dataset_mapping.items()
        for collection_mapping in collections.values()
        for entry in collection_mapping.values()
    )


def _write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """Write JSON to ``path`` via a synced temporary file and an atomic rename."""
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    tmp_path.replace(path)


class _JsonlJournal:
    """
    Thread-safe append-only JSON Lines journal keyed by dataset-relative path.

    Entries are buffered and flushed every ``flush_every`` records or ``flush_interval_s`` seconds, so a hard kill
    loses at most the last few seconds of entries; a lost entry only costs redoing that file's work. A malformed final
    line left by a kill mid-write is skipped on load.
    """

    def __init__(self, path: Path, flush_every: int = 1000, flush_interval_s: float = 5.0) -> None:
        """
        Open the journal at ``path``, loading any entries already recorded.

        Args:
            path: Journal file; created on the first flush.
            flush_every: Flush after this many buffered entries.
            flush_interval_s: Flush when this many seconds have passed since the last flush.
        """
        self._path = path
        self._flush_every = flush_every
        self._flush_interval_s = flush_interval_s
        # Re-entrant so a signal handler running on a thread that is mid-append can still flush.
        self._lock = threading.RLock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._pending: list[str] = []
        self._last_flush = time.monotonic()
        self._load()

    @property
    def path(self) -> Path:
        """Path of the journal file."""
        return self._path

    def __len__(self) -> int:
        return len(self._entries)

    def _load(self) -> None:
        if not self._path.exists():
            return
        skipped = 0
        with self._path.open() as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    self._entries[entry["p"]] = entry
                except (json.JSONDecodeError, KeyError, TypeError):
                    skipped += 1
        if skipped:
            logger.debug(f"Skipped {skipped} malformed journal lines in {self._path.name}")

    def _get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            return self._entries.get(key)

    def _append(self, entry: dict[str, Any]) -> None:
        line = json.dumps(entry, separators=(",", ":"))
        with self._lock:
            self._entries[entry["p"]] = entry
            self._pending.append(line)
            if len(self._pending) >= self._flush_every or time.monotonic() - self._last_flush >= self._flush_interval_s:
                self._flush_locked()

    def _flush_locked(self) -> None:
        if self._pending:
            with self._path.open("a") as f:
                f.write("\n".join(self._pending) + "\n")
                f.flush()
                os.fsync(f.fileno())
            self._pending.clear()
        self._last_flush = time.monotonic()

    def flush(self) -> None:
        """Write any buffered entries to disk."""
        with self._lock:
            self._flush_locked()


class HashJournal(_JsonlJournal):
    """Journal of file hashes, each valid only while the file's size and modification time are unchanged."""

    def lookup(self, rel_path: str, stat: os.stat_result) -> str | None:
        """
        Return the journalled hash for a file, or None when absent or stale.

        Args:
            rel_path: Dataset-relative posix path of the file.
            stat: Current ``os.stat_result`` of the file.

        Returns:
            The SHA-256 hex digest, or None if the file must be hashed.
        """
        entry = self._get(rel_path)
        if entry is None or entry.get("s") != stat.st_size or entry.get("m") != stat.st_mtime_ns:
            return None
        return str(entry["h"])

    def record(self, rel_path: str, stat: os.stat_result, digest: str) -> None:
        """
        Journal a file's hash against the size and modification time it was computed at.

        Args:
            rel_path: Dataset-relative posix path of the file.
            stat: ``os.stat_result`` taken before the file was hashed.
            digest: The SHA-256 hex digest.
        """
        self._append({"p": rel_path, "s": stat.st_size, "m": stat.st_mtime_ns, "h": digest})

    def hash_file(self, path: Path, rel_path: str) -> str:
        """
        Return a file's SHA-256, from the journal when still valid, otherwise computed and journalled.

        Args:
            path: The file to hash.
            rel_path: Dataset-relative posix path used as the journal key.

        Returns:
            The SHA-256 hex digest of the file's contents.
        """
        stat = path.stat()
        cached = self.lookup(rel_path, stat)
        if cached is not None:
            return cached
        digest = compute_hash(path)
        self.record(rel_path, stat, digest)
        return digest


class MetadataFilesJournal(_JsonlJournal):
    """Journal of files whose metadata has been embedded, with the per-item state derived while embedding."""

    def get(self, rel_path: str) -> list[dict[str, Any]] | None:
        """
        Return the derived state recorded for a file, or None if the file has not been processed.

        Args:
            rel_path: Dataset-relative posix path of the file.

        Returns:
            One derived-state dictionary per metadata item of the file, or None.
        """
        entry = self._get(rel_path)
        return None if entry is None else list(entry.get("d", []))

    def record(self, rel_path: str, derived_states: list[dict[str, Any]]) -> None:
        """
        Journal that a file's metadata has been embedded.

        Args:
            rel_path: Dataset-relative posix path of the file.
            derived_states: One derived-state dictionary per metadata item of the file.
        """
        self._append({"p": rel_path, "d": derived_states})


class PackageLock:
    """
    Exclusive packaging lock with a heartbeat.

    The lock file is created exclusively, records the holder's host and process id, and has its modification time
    refreshed every ``heartbeat_s`` seconds while held. A run that finds the lock taken decides whether its holder is
    alive:

    - On the same host, by asking whether the holder's process still exists.
    - On another host, by watching for a heartbeat: a refresh proves the holder alive and the run is refused, while a
      lock not refreshed for ``stale_after_s`` seconds belonged to a run that was killed, and is taken over. A process
      killed outright cannot release its lock, so this wait is what lets a run on another machine resume it.
    """

    def __init__(
        self,
        path: Path,
        heartbeat_s: float = 15.0,
        stale_after_s: float = 120.0,
        poll_s: float = 5.0,
    ) -> None:
        """
        Describe the lock at ``path`` without taking it.

        Args:
            path: Lock file.
            heartbeat_s: Seconds between heartbeats while held.
            stale_after_s: Seconds without a heartbeat after which the lock is stale. Must comfortably exceed
                ``heartbeat_s`` plus the filesystem's attribute-cache lifetime (up to 60 s on NFS).
            poll_s: Seconds between checks while waiting on another host's lock.
        """
        self._path = path
        self._heartbeat_s = heartbeat_s
        self._stale_after_s = stale_after_s
        self._poll_s = poll_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _holder(self) -> dict[str, Any]:
        try:
            return dict(json.loads(self._path.read_text()))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return {}

    @staticmethod
    def _local_process_alive(holder: dict[str, Any]) -> bool | None:
        """Whether a holder on this host is alive; None when the holder is on another host or unidentified."""
        if holder.get("host") != socket.gethostname() or not isinstance(holder.get("pid"), int):
            return None
        try:
            os.kill(holder["pid"], 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _try_create(self, payload: str) -> bool:
        try:
            fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as f:
            f.write(payload)
        return True

    def _held_message(self, holder: dict[str, Any]) -> str:
        return (
            f"Another packaging run holds {self._path} (host {holder.get('host', '?')}, pid {holder.get('pid', '?')}). "
            f"If no run is active, delete the lock file and retry."
        )

    def acquire(self) -> None:
        """
        Take the lock, waiting out or replacing one left by a run that is no longer alive.

        Raises:
            PackageStateError: If another live run holds the lock.
        """
        payload = json.dumps(
            {"host": socket.gethostname(), "pid": os.getpid(), "acquired": datetime.now(UTC).isoformat()},
        )
        first_seen_mtime_ns: int | None = None
        took_over = False
        while not self._try_create(payload):
            try:
                mtime_ns = self._path.stat().st_mtime_ns
            except FileNotFoundError:
                continue
            holder = self._holder()
            alive = self._local_process_alive(holder)
            if alive:
                raise PackageStateError(self._held_message(holder))
            if alive is None and time.time() - mtime_ns / 1e9 <= self._stale_after_s:
                if first_seen_mtime_ns is None:
                    first_seen_mtime_ns = mtime_ns
                    logger.warning(
                        f"Waiting up to {self._stale_after_s:.0f} s for the packaging lock held by host "
                        f"{holder.get('host', '?')} to go stale",
                        extra={"path": str(self._path)},
                    )
                elif mtime_ns != first_seen_mtime_ns:
                    raise PackageStateError(self._held_message(holder))
                time.sleep(self._poll_s)
                continue
            logger.warning("Replacing stale packaging lock", extra={"path": str(self._path)})
            self._path.unlink(missing_ok=True)
            took_over = True

        if took_over:
            # Two runs replacing the same stale lock can both succeed in creating one; the later wins, and the
            # earlier finds its record overwritten.
            time.sleep(min(self._poll_s, 1.0))
            if self._holder().get("pid") != os.getpid() or self._holder().get("host") != socket.gethostname():
                raise PackageStateError(self._held_message(self._holder()))

        self._thread = threading.Thread(target=self._heartbeat, name="package-lock-heartbeat", daemon=True)
        self._thread.start()

    def _heartbeat(self) -> None:
        while not self._stop.wait(self._heartbeat_s):
            with contextlib.suppress(OSError):
                os.utime(self._path)

    def release(self) -> None:
        """Stop the heartbeat and delete the lock file."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._heartbeat_s + 1)
            self._thread = None
        self._path.unlink(missing_ok=True)


class PackageState:
    """
    Checkpoint state for one dataset's packaging run, rooted at ``<project>/.marimba/package/<dataset>/``.

    Use :meth:`create` for a new run and :meth:`load` to resume one. The state holds the packaging lock from creation
    or loading until :meth:`close` or :meth:`finish`.
    """

    RUN_FILE = "run.json"
    LOCK_FILE = "lock"

    def __init__(self, root: Path, run: dict[str, Any]) -> None:
        """
        Wrap loaded or new run state; use :meth:`create` or :meth:`load` rather than calling this directly.

        Args:
            root: State directory.
            run: Contents of ``run.json``.
        """
        self._root = root
        self._run = run
        self._lock = PackageLock(root / self.LOCK_FILE)
        self._hashes: HashJournal | None = None
        self._verified: HashJournal | None = None
        self._metadata_files: MetadataFilesJournal | None = None
        self._finished = False
        self._resumed = False

    @staticmethod
    def exists(root: Path) -> bool:
        """Whether checkpoint state exists at ``root``."""
        return (root / PackageState.RUN_FILE).is_file()

    @classmethod
    def create(
        cls,
        root: Path,
        fingerprint: dict[str, Any],
        destinations: dict[str, Any],
        completed_stages: Iterable[PackageStage] = (),
    ) -> "PackageState":
        """
        Create fresh checkpoint state for a run and take the packaging lock.

        Args:
            root: State directory, created if absent; any previous state in it is discarded.
            fingerprint: The run's inputs (see :meth:`fingerprint_differences`).
            destinations: Destination digest from :func:`destinations_digest`.
            completed_stages: Stages to record as already complete (used to adopt a run that predates checkpointing).

        Returns:
            The new state.
        """
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True)
        run = {
            "format": STATE_FORMAT_VERSION,
            "fingerprint": fingerprint,
            "destinations": destinations,
            "started_at": datetime.now(UTC).isoformat(),
            "stages": {str(int(stage)): "done" for stage in completed_stages},
        }
        state = cls(root, run)
        state._resumed = bool(run["stages"])
        state._lock.acquire()
        state._save()
        return state

    @classmethod
    def load(cls, root: Path) -> "PackageState":
        """
        Load existing checkpoint state and take the packaging lock.

        Args:
            root: State directory.

        Returns:
            The loaded state.

        Raises:
            PackageStateError: If the state is unreadable, of an unknown format, or locked by a live run.
        """
        try:
            run = json.loads((root / cls.RUN_FILE).read_text())
        except (OSError, json.JSONDecodeError) as e:
            msg = f"Cannot read packaging state at {root}: {e}"
            raise PackageStateError(msg) from e
        if run.get("format") != STATE_FORMAT_VERSION:
            msg = f"Packaging state at {root} has unsupported format {run.get('format')!r}"
            raise PackageStateError(msg)
        state = cls(root, run)
        state._resumed = True
        state._lock.acquire()
        return state

    @property
    def root(self) -> Path:
        """State directory."""
        return self._root

    @property
    def resumed(self) -> bool:
        """Whether this run continues an earlier attempt, so the dataset may hold that attempt's partial work."""
        return self._resumed

    @property
    def started_at(self) -> datetime:
        """When the run was first started (preserved across resumes)."""
        return datetime.fromisoformat(self._run["started_at"])

    @property
    def fingerprint(self) -> dict[str, Any]:
        """The recorded fingerprint of the run's inputs."""
        return dict(self._run["fingerprint"])

    @property
    def destinations(self) -> dict[str, Any]:
        """The recorded destination digest."""
        return dict(self._run["destinations"])

    def fingerprint_differences(self, fingerprint: dict[str, Any]) -> list[str]:
        """
        Compare a fingerprint against the recorded one.

        Args:
            fingerprint: The fingerprint of the run being resumed.

        Returns:
            One ``"key: recorded -> current"`` line per differing field; empty when they match.
        """
        recorded = self._run["fingerprint"]
        keys = sorted(set(recorded) | set(fingerprint))
        return [
            f"{key}: {recorded.get(key)!r} -> {fingerprint.get(key)!r}"
            for key in keys
            if recorded.get(key) != fingerprint.get(key)
        ]

    def is_done(self, stage: PackageStage) -> bool:
        """Whether ``stage`` completed in this or an earlier attempt."""
        return bool(self._run["stages"].get(str(int(stage))) == "done")

    def mark_done(self, stage: PackageStage) -> None:
        """Record ``stage`` as complete, flushing every journal first so the record never runs ahead of them."""
        self.flush()
        self._run["stages"][str(int(stage))] = "done"
        self._save()
        logger.debug(f"Packaging stage {int(stage)} ({stage.label}) checkpointed")

    def reset_from(self, stage: PackageStage) -> None:
        """Record every stage before ``stage`` as complete and ``stage`` onwards as pending."""
        self._run["stages"] = {str(int(s)): "done" for s in PackageStage if s < stage}
        self._save()

    @property
    def completed_stages(self) -> list[PackageStage]:
        """Completed stages in order."""
        return [stage for stage in PackageStage if self.is_done(stage)]

    @property
    def hashes(self) -> HashJournal:
        """Hash journal shared by metadata generation and manifest generation."""
        if self._hashes is None:
            self._hashes = HashJournal(self._root / "hashes.jsonl")
        return self._hashes

    @property
    def verified(self) -> HashJournal:
        """Hash journal for the validation pass."""
        if self._verified is None:
            self._verified = HashJournal(self._root / "verified.jsonl")
        return self._verified

    @property
    def metadata_files(self) -> MetadataFilesJournal:
        """Journal of files whose metadata has been embedded."""
        if self._metadata_files is None:
            self._metadata_files = MetadataFilesJournal(self._root / "metadata_files.jsonl")
        return self._metadata_files

    def flush(self) -> None:
        """Write buffered journal entries to disk."""
        for journal in (self._hashes, self._verified, self._metadata_files):
            if journal is not None:
                journal.flush()

    def _save(self) -> None:
        self._run["updated_at"] = datetime.now(UTC).isoformat()
        _write_json_atomic(self._root / self.RUN_FILE, self._run)

    def close(self) -> None:
        """Flush journals and release the lock, keeping the state for a later resume. A no-op after :meth:`finish`."""
        if self._finished:
            return
        self.flush()
        self._lock.release()

    def finish(self) -> None:
        """Release the lock and delete the state after a successful run."""
        self._finished = True
        self._lock.release()
        shutil.rmtree(self._root, ignore_errors=True)
