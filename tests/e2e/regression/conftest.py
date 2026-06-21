"""Fixtures for the mritc-demo end-to-end regression harness.

Cache layout (default `<repo>/.cache/marimba-e2e/`; override via `MARIMBA_E2E_CACHE_DIR`
to share a cache across worktrees / CI runners):

    <cache>/data/        # git clone of mritc-demo-data, pinned to DATA_REPO_SHA
    <cache>/pipeline/    # git clone of mritc-demo-pipeline, pinned to PIPELINE_REPO_SHA
    <cache>/.lockfiles/  # file locks coordinating concurrent fixture access

Both caches are populated lazily on first use and reused across runs. To force
a refresh, delete the relevant subdirectory (or the whole cache). The repo's
`.git/info/exclude` hides `.cache/` from git status; see CLAUDE.md §0.2.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

# Pinned upstream SHAs. Bump deliberately when we choose to refresh the
# fixture surface; goldens must be regenerated whenever either bumps.
DATA_REPO_URL = "https://github.com/csiro-fair/mritc-demo-data"
DATA_REPO_SHA = "804733fcbda7404b4776a80ac55dcd91bebd7ead"

PIPELINE_REPO_URL = "https://github.com/csiro-fair/mritc-demo-pipeline"
PIPELINE_REPO_SHA = "ad1334df520e07f8d824489d4ff0ed906a360b2c"

# Each top-level directory in the data repo is one collection. The marimba
# convention for this dataset prefixes the directory name with `IN2018_V06_`.
COLLECTION_SOURCE_DIRS = (
    "025",
    "026",
    "045",
    "057",
    "060",
    "064",
    "114",
    "119",
    "128",
    "168",
)
COLLECTION_PREFIX = "IN2018_V06_"


_REPO_ROOT = Path(__file__).resolve().parents[3]


def _cache_root() -> Path:
    """Resolve the on-disk cache root, honoring MARIMBA_E2E_CACHE_DIR override."""
    override = os.environ.get("MARIMBA_E2E_CACHE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return _REPO_ROOT / ".cache" / "marimba-e2e"


@contextmanager
def _file_lock(lock_path: Path) -> Iterator[None]:
    """Coordinate concurrent fixture access via an advisory exclusive lock."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _git(*args: str, cwd: Path | None = None) -> None:
    """Run git with the requested args; raise on non-zero exit."""
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)  # noqa: S607


def _ensure_repo_at_sha(repo_dir: Path, url: str, sha: str, lock_path: Path) -> Path:
    """Clone `url` into `repo_dir` if missing and check out `sha`.

    Reuses an existing clone if its HEAD already matches `sha`. The advisory
    lock keeps concurrent pytest workers from racing on the same cache dir.
    """
    with _file_lock(lock_path):
        if repo_dir.exists():
            try:
                head = subprocess.run(
                    ["git", "rev-parse", "HEAD"],  # noqa: S607
                    cwd=repo_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                if head != sha:
                    # HEAD drifted — fetch + reset to the pinned SHA.
                    _git("fetch", "--depth=1", "origin", sha, cwd=repo_dir)
                    _git("reset", "--hard", sha, cwd=repo_dir)
            except subprocess.CalledProcessError:
                # Repo is corrupt — wipe and reclone below.
                shutil.rmtree(repo_dir)
            else:
                return repo_dir

        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        # `--filter=tree:0` minimises history fetch; we only need the snapshot.
        _git("clone", "--filter=tree:0", url, str(repo_dir))
        _git("fetch", "--depth=1", "origin", sha, cwd=repo_dir)
        _git("checkout", sha, cwd=repo_dir)
        return repo_dir


@pytest.fixture(scope="session")
def e2e_cache_root() -> Path:
    """Cache root path; created if missing."""
    root = _cache_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture(scope="session")
def cached_data(e2e_cache_root: Path) -> Path:
    """Path to the cached mritc-demo-data clone at the pinned SHA."""
    return _ensure_repo_at_sha(
        repo_dir=e2e_cache_root / "data",
        url=DATA_REPO_URL,
        sha=DATA_REPO_SHA,
        lock_path=e2e_cache_root / ".lockfiles" / "data.lock",
    )


@pytest.fixture(scope="session")
def cached_pipeline(e2e_cache_root: Path) -> Path:
    """Path to the cached mritc-demo-pipeline clone at the pinned SHA."""
    return _ensure_repo_at_sha(
        repo_dir=e2e_cache_root / "pipeline",
        url=PIPELINE_REPO_URL,
        sha=PIPELINE_REPO_SHA,
        lock_path=e2e_cache_root / ".lockfiles" / "pipeline.lock",
    )


@pytest.fixture(scope="module")
def scratch_project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Module-scoped scratch path for a fresh marimba project.

    Returns a `<unique-tmp>/project` path that does not yet exist; the test
    or the `marimba new project` invocation creates it. Module scope means
    every test in a single test file shares one project — the regression
    tests under this directory are read-only against the packaged dataset
    so they don't need per-test isolation, and sharing one bootstrap drops
    wall-clock from ~30s x N tests to ~30s once per module. pytest cleans
    up the tmp tree at session end.
    """
    return tmp_path_factory.mktemp("scratch") / "project"


@pytest.fixture(scope="module")
def marimba_run(scratch_project: Path) -> MarimbaRunner:
    """Subprocess-based marimba CLI invoker scoped to scratch_project.

    Uses subprocess rather than typer.testing.CliRunner because the pipeline
    install path imports the cloned pipeline module via importlib, which
    interacts poorly with CliRunner's in-process invocation and module cache.
    Module-scoped to match scratch_project (one runner per test file).
    """
    return MarimbaRunner(project_dir=scratch_project)


class MarimbaRunner:
    """Thin wrapper around `uv run marimba ...` for the regression harness."""

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir
        self._repo_root = _REPO_ROOT

    def run(self, *args: str, timeout: float = 600.0) -> subprocess.CompletedProcess[str]:
        """Invoke marimba with the given args; returns the completed process.

        Captures stdout/stderr as text. On non-zero exit, raises a
        RuntimeError whose message embeds the captured streams so failures
        are diagnosable in CI logs (subprocess.CalledProcessError.__str__
        only includes cmd + exit code, swallowing the stdout/stderr that
        contains the actual marimba error).
        """
        cmd = ["uv", "run", "marimba", *args]
        result = subprocess.run(
            cmd,
            cwd=self._repo_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode != 0:
            msg = (
                f"marimba command failed (exit {result.returncode}): {' '.join(cmd)}\n"
                f"=== STDOUT ===\n{result.stdout}\n"
                f"=== STDERR ===\n{result.stderr}"
            )
            raise RuntimeError(msg)
        return result

    def new_project(self) -> subprocess.CompletedProcess[str]:
        return self.run("new", "project", str(self.project_dir))

    def new_pipeline(self, name: str, source: Path | str) -> subprocess.CompletedProcess[str]:
        return self.run(
            "new",
            "pipeline",
            name,
            str(source),
            "--project-dir",
            str(self.project_dir),
            "--accept-defaults",
        )

    def import_collection(self, name: str, source: Path) -> subprocess.CompletedProcess[str]:
        return self.run(
            "import",
            name,
            str(source),
            "--project-dir",
            str(self.project_dir),
        )

    def process(self) -> subprocess.CompletedProcess[str]:
        return self.run("process", "--project-dir", str(self.project_dir), timeout=1200)

    def package(self, dataset_name: str) -> subprocess.CompletedProcess[str]:
        return self.run(
            "package",
            dataset_name,
            "--project-dir",
            str(self.project_dir),
            "--operation",
            "link",
            "--version",
            "1.0",
            "--contact-name",
            "Test Contact",
            "--contact-email",
            "test@example.com",
            "--metadata-level",
            "project",
            "--metadata-level",
            "pipeline",
            "--metadata-level",
            "collection",
            "--zoom",
            "9",
            timeout=1200,
        )


def preinstall_pipeline_requirements(pipeline_repo: Path) -> None:
    """Install a pipeline's requirements.txt into marimba's venv before `new pipeline`.

    Workaround for a marimba lifecycle ordering bug: `marimba new pipeline`
    dynamic-loads the pipeline module during config-prompt introspection
    (`PipelineWrapper.prompt_pipeline_config -> get_instance -> importlib`),
    so any pipeline whose top-level imports include a non-marimba dep
    (mritc_demo.pipeline.py imports pandas) fails the import before the
    separate `marimba install` command gets a chance to install
    requirements.txt. The dev-loop "works" only when the developer's venv
    already carries the deps from a prior pipeline install — fresh-venv
    setups (and CI) are broken.

    Pre-install via `uv pip install -r ...` against the cwd-resolved venv
    (the same venv `uv run marimba` uses). Worth flagging as a codebase-
    review finding for a future cycle.
    """
    req = pipeline_repo / "requirements.txt"
    if not req.is_file():
        return
    subprocess.run(
        ["uv", "pip", "install", "-r", str(req)],  # noqa: S607
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )


# Dataset name used by the package fixture. Stays in sync with the goldens.
PACKAGED_DATASET_NAME = "IN2018_V06"


@pytest.fixture(scope="module")
def imported_project(
    marimba_run: MarimbaRunner,
    cached_data: Path,
    cached_pipeline: Path,
) -> tuple[Path, dict[str, float]]:
    """A fresh marimba project with the MRITC pipeline + all 10 collections imported.

    Returns (project_dir, timings) where timings is a dict of per-stage
    wall-clock seconds for the calling test to report on.

    Performs `new project`, `new pipeline`, and 10x `import` against the
    cached data. Stops before `process` / `package` — use `packaged_dataset`
    for the full cycle. Module-scoped: one import sequence per test file.
    """
    timings: dict[str, float] = {}

    t0 = time.monotonic()
    marimba_run.new_project()
    timings["new_project_s"] = time.monotonic() - t0

    t0 = time.monotonic()
    preinstall_pipeline_requirements(cached_pipeline)
    timings["preinstall_pipeline_deps_s"] = time.monotonic() - t0

    t0 = time.monotonic()
    marimba_run.new_pipeline("MRITC", cached_pipeline)
    timings["new_pipeline_s"] = time.monotonic() - t0

    t0 = time.monotonic()
    for src_dir in COLLECTION_SOURCE_DIRS:
        marimba_run.import_collection(
            name=f"{COLLECTION_PREFIX}{src_dir}",
            source=cached_data / src_dir,
        )
    timings["import_all_s"] = time.monotonic() - t0

    return marimba_run.project_dir, timings


@pytest.fixture(scope="module")
def packaged_dataset(
    marimba_run: MarimbaRunner,
    imported_project: tuple[Path, dict[str, float]],
) -> tuple[Path, dict[str, float]]:
    """Full marimba cycle: imported_project + `process` + `package`.

    Returns (dataset_dir, timings) where dataset_dir is
    `<project>/datasets/<PACKAGED_DATASET_NAME>` and timings extends the
    imported_project timings dict with `process_s` and `package_s` entries.

    Module-scoped: every test in the calling test file shares one packaged
    dataset. The regression tests are read-only against the dataset so this
    is safe; sharing the bootstrap cuts wall-clock from ~30s x N to ~30s
    once. If a future test needs an isolated packaged dataset (e.g. to
    mutate then re-package), override this fixture at function scope in
    that test file.
    """
    project_dir, timings = imported_project

    t0 = time.monotonic()
    marimba_run.process()
    timings["process_s"] = time.monotonic() - t0

    t0 = time.monotonic()
    marimba_run.package(PACKAGED_DATASET_NAME)
    timings["package_s"] = time.monotonic() - t0

    dataset_dir = project_dir / "datasets" / PACKAGED_DATASET_NAME
    return dataset_dir, timings
