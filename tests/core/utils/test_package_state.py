"""Tests for the packaging checkpoint state: journals, lock and run state."""

import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from marimba.core.utils.hash import compute_hash
from marimba.core.utils.package_state import (
    HashJournal,
    MetadataFilesJournal,
    PackageLock,
    PackageStage,
    PackageState,
    PackageStateError,
    destinations_digest,
    mapping_destinations_digest,
)


@pytest.fixture
def state_root(tmp_path: Path) -> Path:
    return tmp_path / ".marimba" / "package" / "dataset"


def _write(path: Path, content: bytes) -> os.stat_result:
    path.write_bytes(content)
    return path.stat()


class TestDestinationsDigest:
    """The order-independent digest of a run's destination paths."""

    @pytest.mark.unit
    def test_order_independent(self) -> None:
        assert destinations_digest(["b", "a"]) == destinations_digest(["a", "b"])

    @pytest.mark.unit
    def test_count_and_content_sensitive(self) -> None:
        digest = destinations_digest(["a", "b"])
        assert digest["count"] == 2
        assert digest != destinations_digest(["a", "c"])
        # The separator stops path boundaries from shifting without changing the digest.
        assert destinations_digest(["ab", "c"]) != destinations_digest(["a", "bc"])

    @pytest.mark.unit
    def test_mapping_digest_uses_pipeline_and_destination(self) -> None:
        mapping = {
            "P": {
                "C1": {Path("/src/a.jpg"): (Path("C1/a.jpg"), None, None)},
                "C2": {Path("/src/b.jpg"): (Path("C2/b.jpg"), None, None)},
            },
        }
        assert mapping_destinations_digest(mapping) == destinations_digest(["P/C1/a.jpg", "P/C2/b.jpg"])


class TestHashJournal:
    """Hash journal lookups, staleness and persistence."""

    @pytest.mark.unit
    def test_lookup_returns_hash_for_unchanged_file(self, tmp_path: Path) -> None:
        stat = _write(tmp_path / "f.bin", b"abc")
        journal = HashJournal(tmp_path / "hashes.jsonl")
        journal.record("f.bin", stat, "H")
        assert journal.lookup("f.bin", stat) == "H"

    @pytest.mark.unit
    def test_lookup_misses_when_size_or_mtime_changes(self, tmp_path: Path) -> None:
        path = tmp_path / "f.bin"
        stat = _write(path, b"abc")
        journal = HashJournal(tmp_path / "hashes.jsonl")
        journal.record("f.bin", stat, "H")

        resized = _write(path, b"abcd")
        assert journal.lookup("f.bin", resized) is None

        _write(path, b"abc")
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        assert journal.lookup("f.bin", path.stat()) is None

    @pytest.mark.unit
    def test_entries_survive_reload_after_flush(self, tmp_path: Path) -> None:
        stat = _write(tmp_path / "f.bin", b"abc")
        journal = HashJournal(tmp_path / "hashes.jsonl")
        journal.record("f.bin", stat, "H")
        journal.flush()

        reloaded = HashJournal(tmp_path / "hashes.jsonl")
        assert reloaded.lookup("f.bin", stat) == "H"
        assert len(reloaded) == 1

    @pytest.mark.unit
    def test_unflushed_entries_are_buffered(self, tmp_path: Path) -> None:
        stat = _write(tmp_path / "f.bin", b"abc")
        journal = HashJournal(tmp_path / "hashes.jsonl", flush_every=10, flush_interval_s=3600)
        journal.record("f.bin", stat, "H")
        assert not (tmp_path / "hashes.jsonl").exists()
        journal.flush()
        assert (tmp_path / "hashes.jsonl").exists()

    @pytest.mark.unit
    def test_flushes_when_buffer_fills(self, tmp_path: Path) -> None:
        stat = _write(tmp_path / "f.bin", b"abc")
        journal = HashJournal(tmp_path / "hashes.jsonl", flush_every=2, flush_interval_s=3600)
        journal.record("a", stat, "A")
        journal.record("b", stat, "B")
        assert len((tmp_path / "hashes.jsonl").read_text().splitlines()) == 2

    @pytest.mark.unit
    def test_torn_trailing_line_is_skipped(self, tmp_path: Path) -> None:
        stat = _write(tmp_path / "f.bin", b"abc")
        journal_path = tmp_path / "hashes.jsonl"
        entry = {"p": "f.bin", "s": stat.st_size, "m": stat.st_mtime_ns, "h": "H"}
        journal_path.write_text(json.dumps(entry) + "\n" + '{"p": "g.bin", "s": 3, "m"')

        journal = HashJournal(journal_path)
        assert journal.lookup("f.bin", stat) == "H"
        assert len(journal) == 1

    @pytest.mark.unit
    def test_hash_file_computes_once_then_reuses(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "f.bin"
        path.write_bytes(b"abc")
        calls: list[Path] = []
        real_compute_hash = compute_hash

        def counting_compute_hash(p: Path, root_dir: Path | None = None) -> str:
            calls.append(p)
            return real_compute_hash(p, root_dir)

        monkeypatch.setattr("marimba.core.utils.package_state.compute_hash", counting_compute_hash)
        journal = HashJournal(tmp_path / "hashes.jsonl")

        first = journal.hash_file(path, "f.bin")
        second = journal.hash_file(path, "f.bin")

        assert first == second == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        assert calls == [path]


class TestMetadataFilesJournal:
    """The journal of files whose metadata has been embedded."""

    @pytest.mark.unit
    def test_records_and_reloads_derived_state(self, tmp_path: Path) -> None:
        journal = MetadataFilesJournal(tmp_path / "metadata_files.jsonl")
        assert journal.get("a.jpg") is None
        journal.record("a.jpg", [{"image_entropy": 1.5}])
        journal.flush()

        reloaded = MetadataFilesJournal(tmp_path / "metadata_files.jsonl")
        assert reloaded.get("a.jpg") == [{"image_entropy": 1.5}]


class TestPackageLock:
    """Exclusive packaging lock with heartbeat and stale takeover."""

    @pytest.mark.unit
    def test_second_acquire_is_refused_while_held(self, tmp_path: Path) -> None:
        first = PackageLock(tmp_path / "lock")
        first.acquire()
        try:
            with pytest.raises(PackageStateError, match="Another packaging run holds"):
                PackageLock(tmp_path / "lock").acquire()
        finally:
            first.release()
        assert not (tmp_path / "lock").exists()

    @pytest.mark.unit
    def test_lock_without_heartbeat_goes_stale(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "lock"
        lock_path.write_text(json.dumps({"host": "elsewhere", "pid": 1}))
        old = time.time() - 600
        os.utime(lock_path, (old, old))

        lock = PackageLock(lock_path)
        lock.acquire()
        try:
            assert json.loads(lock_path.read_text())["pid"] == os.getpid()
        finally:
            lock.release()

    @pytest.mark.unit
    def test_lock_of_dead_local_process_is_stale(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        lock_path = tmp_path / "lock"
        lock_path.write_text(json.dumps({"host": socket.gethostname(), "pid": 4_000_000}))

        def no_such_process(pid: int, signal: int) -> None:  # noqa: ARG001
            raise ProcessLookupError

        monkeypatch.setattr("marimba.core.utils.package_state.os.kill", no_such_process)
        lock = PackageLock(lock_path)
        lock.acquire()
        lock.release()

    @pytest.mark.unit
    def test_live_lock_on_another_host_is_refused_once_its_heartbeat_is_seen(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "lock"
        lock_path.write_text(json.dumps({"host": "other-node", "pid": 1}))
        stop = threading.Event()

        def heartbeat() -> None:
            while not stop.wait(0.05):
                os.utime(lock_path)

        thread = threading.Thread(target=heartbeat)
        thread.start()
        try:
            with pytest.raises(PackageStateError, match="host other-node"):
                PackageLock(lock_path, stale_after_s=30, poll_s=0.1).acquire()
        finally:
            stop.set()
            thread.join()

    @pytest.mark.unit
    def test_silent_lock_on_another_host_is_taken_over_after_waiting(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "lock"
        lock_path.write_text(json.dumps({"host": "other-node", "pid": 1}))

        start = time.monotonic()
        lock = PackageLock(lock_path, stale_after_s=0.5, poll_s=0.05)
        lock.acquire()
        try:
            assert time.monotonic() - start >= 0.4, "a fresh lock is waited on before it is taken over"
            assert json.loads(lock_path.read_text())["pid"] == os.getpid()
        finally:
            lock.release()

    @pytest.mark.unit
    def test_heartbeat_refreshes_lock(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "lock"
        lock = PackageLock(lock_path, heartbeat_s=0.05)
        lock.acquire()
        try:
            old = time.time() - 600
            os.utime(lock_path, (old, old))
            time.sleep(0.3)
            assert time.time() - lock_path.stat().st_mtime < 60
        finally:
            lock.release()


class TestPackageState:
    """PackageState lifecycle, stage records and fingerprint comparison."""

    @pytest.mark.unit
    def test_create_then_load_round_trips(self, state_root: Path) -> None:
        state = PackageState.create(state_root, {"dataset": "D"}, {"count": 1, "sha256": "x"})
        state.mark_done(PackageStage.POPULATE)
        state.close()

        assert PackageState.exists(state_root)
        loaded = PackageState.load(state_root)
        try:
            assert loaded.is_done(PackageStage.POPULATE)
            assert not loaded.is_done(PackageStage.PROCESS_METADATA_FILES)
            assert loaded.fingerprint == {"dataset": "D"}
            assert loaded.destinations == {"count": 1, "sha256": "x"}
            assert loaded.started_at == state.started_at
        finally:
            loaded.close()

    @pytest.mark.unit
    def test_state_holds_lock_until_closed(self, state_root: Path) -> None:
        state = PackageState.create(state_root, {}, {})
        try:
            with pytest.raises(PackageStateError):
                PackageState.load(state_root)
        finally:
            state.close()
        PackageState.load(state_root).close()

    @pytest.mark.unit
    def test_create_discards_previous_state(self, state_root: Path) -> None:
        state = PackageState.create(state_root, {}, {})
        state.mark_done(PackageStage.POPULATE)
        state.close()

        fresh = PackageState.create(state_root, {}, {})
        try:
            assert fresh.completed_stages == []
        finally:
            fresh.close()

    @pytest.mark.unit
    def test_create_records_adopted_stages(self, state_root: Path) -> None:
        completed = [stage for stage in PackageStage if stage < PackageStage.MANIFEST]
        state = PackageState.create(state_root, {}, {}, completed_stages=completed)
        try:
            assert state.completed_stages == completed
        finally:
            state.close()

    @pytest.mark.unit
    def test_reset_from_marks_earlier_stages_done_and_later_pending(self, state_root: Path) -> None:
        state = PackageState.create(state_root, {}, {})
        try:
            state.mark_done(PackageStage.SUMMARY)
            state.reset_from(PackageStage.SUMMARY)
            assert state.is_done(PackageStage.DATASET_METADATA)
            assert not state.is_done(PackageStage.SUMMARY)
        finally:
            state.close()

    @pytest.mark.unit
    def test_fingerprint_differences_lists_each_changed_field(self, state_root: Path) -> None:
        state = PackageState.create(state_root, {"a": 1, "b": [1], "c": "x"}, {})
        try:
            assert state.fingerprint_differences({"a": 1, "b": [1], "c": "x"}) == []
            assert state.fingerprint_differences({"a": 2, "b": [1], "d": True}) == [
                "a: 1 -> 2",
                "c: 'x' -> None",
                "d: None -> True",
            ]
        finally:
            state.close()

    @pytest.mark.unit
    def test_mark_done_flushes_journals_first(self, state_root: Path, tmp_path: Path) -> None:
        stat = _write(tmp_path / "f.bin", b"abc")
        state = PackageState.create(state_root, {}, {})
        try:
            state.hashes.record("f.bin", stat, "H")
            state.mark_done(PackageStage.DATASET_METADATA)
            assert HashJournal(state_root / "hashes.jsonl").lookup("f.bin", stat) == "H"
        finally:
            state.close()

    @pytest.mark.unit
    def test_finish_removes_state_and_close_is_then_a_no_op(self, state_root: Path) -> None:
        state = PackageState.create(state_root, {}, {})
        state.hashes  # noqa: B018 - open a journal so close would have something to flush
        state.finish()
        state.close()
        assert not state_root.exists()

    @pytest.mark.unit
    def test_load_rejects_unknown_format(self, state_root: Path) -> None:
        state_root.mkdir(parents=True)
        (state_root / "run.json").write_text(json.dumps({"format": 999}))
        with pytest.raises(PackageStateError, match="unsupported format"):
            PackageState.load(state_root)

    @pytest.mark.unit
    def test_load_rejects_unreadable_state(self, state_root: Path) -> None:
        state_root.mkdir(parents=True)
        (state_root / "run.json").write_text("{not json")
        with pytest.raises(PackageStateError, match="Cannot read packaging state"):
            PackageState.load(state_root)

    @pytest.mark.unit
    def test_stage_labels_and_numbers_match_progress_bars(self) -> None:
        assert int(PackageStage.MANIFEST) == 10
        assert PackageStage.PROCESS_METADATA_FILES.label == "process metadata files"
