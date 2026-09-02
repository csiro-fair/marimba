"""
Marimba provenance record.

Builds a W3C PROV-O provenance document (serialised as JSON-LD) for a packaged Marimba dataset. The packaging run
is modelled as a ``prov:Activity`` that generated the dataset (a ``prov:Entity``) and was associated with the
software agents involved: Marimba itself, each processing pipeline (with its git provenance), and the external
tools (ExifTool, FFmpeg) whose versions materially affect the output.

The vocabulary is restricted to PROV-O and schema.org so that any JSON-LD consumer can interpret the record without
a Marimba-specific context. A git revision is identified three ways: the browsable repository
(``schema:codeRepository``), a permalink to the exact commit (``schema:url``), and the commit's Software Heritage
persistent identifier (``schema:identifier``, ``swh:1:rev:<sha>``), which is intrinsic to the commit and so remains
valid whether or not the repository is archived. Every node is given an absolute IRI under the dataset's
``urn:uuid:`` so the graph is globally unique and independent of where the file is stored.

The record is best-effort: git metadata is read from each pipeline's source repository when present, and tool
versions are queried at packaging time; anything unavailable is simply omitted rather than failing the package.
"""

import contextlib
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from marimba.core.utils.log import get_logger

logger = get_logger(__name__)

MARIMBA_REPOSITORY_URL = "https://github.com/csiro-fair/marimba"

_PROV_CONTEXT: dict[str, str] = {
    "prov": "http://www.w3.org/ns/prov#",
    "schema": "http://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}

# scp-style git remote, e.g. git@github.com:org/repo.git (a user prefix is required to rule out local paths).
_SCP_REMOTE_RE = re.compile(r"^[\w.-]+@([\w.-]+):/?(.+)$")
_URL_SCHEMES = frozenset({"http", "https", "ssh", "git", "git+ssh", "ssh+git"})
# A plain release version (no dev / local segments), i.e. one that exists on PyPI and as a git tag.
_RELEASE_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")

_PIPELINE_DIRTY_DESCRIPTION = (
    "The pipeline working tree contained uncommitted changes to tracked files at packaging time; the pipeline "
    "source copied into the dataset's pipelines/ directory is authoritative."
)
_MARIMBA_DIRTY_DESCRIPTION = (
    "Marimba was run from a source checkout containing uncommitted changes to tracked files at packaging time."
)


def normalise_repository_url(url: str) -> str | None:
    """
    Return a browsable https URL for a git remote, or None if the remote is not a real remote (e.g. a local path).

    Accepts http(s), ssh and git schemes as well as scp-style ``user@host:org/repo.git`` remotes. Credentials, ports,
    the ``.git`` suffix and trailing slashes are stripped so the result is suitable as ``schema:codeRepository`` and
    as the base for a commit permalink.
    """
    if "://" in url:
        parts = urlsplit(url)
        if parts.scheme not in _URL_SCHEMES or not parts.hostname:
            return None
        scheme = "http" if parts.scheme == "http" else "https"
        host, path = parts.hostname, parts.path
    else:
        match = _SCP_REMOTE_RE.match(url)
        if not match:
            return None
        scheme, host, path = "https", match.group(1), "/" + match.group(2)
    path = path.rstrip("/")
    path = path.removesuffix(".git").rstrip("/")
    if not path:
        return None
    return f"{scheme}://{host}{path}"


def _commit_url(repository_url: str, commit: str) -> str:
    """Web permalink to a commit: Bitbucket uses ``/commits/<sha>``; GitHub, GitLab, Gitea and Codeberg ``/commit/``."""
    segment = "commits" if urlsplit(repository_url).hostname == "bitbucket.org" else "commit"
    return f"{repository_url}/{segment}/{commit}"


def _read_git_info(repo_dir: Path) -> dict[str, Any]:
    """Read commit, repository URL, release tag and dirty state from a git repository (best-effort)."""
    info: dict[str, Any] = {}
    try:
        from git import GitCommandError, InvalidGitRepositoryError, NoSuchPathError, Repo  # noqa: PLC0415

        try:
            repo = Repo(repo_dir)
        except (InvalidGitRepositoryError, NoSuchPathError):
            logger.debug(f"No git repository at {repo_dir}")
            return info

        try:
            info["commit"] = repo.head.commit.hexsha
            with contextlib.suppress(AttributeError, ValueError):
                # Only record a real remote URL, never a local clone path (e.g. an offline cache).
                url = normalise_repository_url(repo.remotes.origin.url)
                if url:
                    info["url"] = url
            # Record a release tag only when the commit is exactly on one.
            with contextlib.suppress(GitCommandError):
                info["tag"] = repo.git.describe("--tags", "--exact-match")
            # Uncommitted changes to tracked files mean the code differs from the recorded commit.
            info["dirty"] = repo.is_dirty()
        finally:
            repo.close()
    except Exception:
        logger.exception(f"Failed to read git metadata at {repo_dir}")
    return info


def _marimba_git_info() -> dict[str, Any]:
    """Best-effort git info for Marimba itself when running from a source checkout (empty if pip-installed)."""
    # marimba/core/utils/provenance.py -> repo root is parents[3].
    return _read_git_info(Path(__file__).resolve().parents[3])


def _git_properties(git_info: dict[str, Any], dirty_description: str) -> dict[str, Any]:
    """schema.org properties identifying the git revision described by ``git_info``."""
    properties: dict[str, Any] = {}
    url, commit = git_info.get("url"), git_info.get("commit")
    if url:
        properties["schema:codeRepository"] = url
    if commit:
        properties["schema:identifier"] = f"swh:1:rev:{commit}"
        if url:
            properties["schema:url"] = _commit_url(url, commit)
    if "tag" in git_info:
        properties["schema:softwareVersion"] = git_info["tag"]
    if git_info.get("dirty"):
        properties["schema:description"] = dirty_description
    return properties


def _run_version(command: list[str]) -> str | None:
    """Run a tool's version command and return its stdout, or None if the tool is unavailable."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=10)
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def _exiftool_version() -> str | None:
    """Return the installed ExifTool version, or None if unavailable."""
    return _run_version(["exiftool", "-ver"])


def _ffmpeg_version() -> str | None:
    """Return the installed FFmpeg version, or None if unavailable."""
    output = _run_version(["ffmpeg", "-version"])
    if not output:
        return None
    # First line is e.g. "ffmpeg version 6.1.1 Copyright (c) ...".
    parts = output.splitlines()[0].split()
    if len(parts) >= 3 and parts[0] == "ffmpeg" and parts[1] == "version":  # noqa: PLR2004
        return parts[2]
    return None


def _xsd_datetime(value: datetime) -> dict[str, str]:
    """Typed JSON-LD literal for a UTC timestamp."""
    return {"@type": "xsd:dateTime", "@value": value.strftime("%Y-%m-%dT%H:%M:%SZ")}


def _pipeline_agent(dataset_iri: str, name: str, repo_dir: Path) -> dict[str, Any]:
    """Build a pipeline software agent, enriched with whatever git provenance is available."""
    agent: dict[str, Any] = {
        "@id": f"{dataset_iri}#pipeline-{name}",
        "@type": ["prov:SoftwareAgent", "schema:SoftwareSourceCode"],
        "schema:name": name,
    }
    agent.update(_git_properties(_read_git_info(repo_dir), _PIPELINE_DIRTY_DESCRIPTION))
    return agent


def _tool_agents(dataset_iri: str) -> list[dict[str, Any]]:
    """Build software agents for the external tools whose versions are available."""
    agents: list[dict[str, Any]] = []
    for fragment, name, version in (
        ("exiftool", "ExifTool", _exiftool_version()),
        ("ffmpeg", "FFmpeg", _ffmpeg_version()),
    ):
        if version:
            agents.append(
                {
                    "@id": f"{dataset_iri}#{fragment}",
                    "@type": ["prov:SoftwareAgent", "schema:SoftwareApplication"],
                    "schema:name": name,
                    "schema:softwareVersion": version,
                },
            )
    return agents


def _marimba_agent(dataset_iri: str, marimba_version: str) -> dict[str, Any]:
    """Build the Marimba software agent, with its own git commit when run from a checkout."""
    agent: dict[str, Any] = {
        "@id": f"{dataset_iri}#marimba",
        "@type": ["prov:SoftwareAgent", "schema:SoftwareApplication"],
        "schema:name": "Marimba",
        "schema:softwareVersion": marimba_version,
        "schema:codeRepository": MARIMBA_REPOSITORY_URL,
    }
    is_release = bool(_RELEASE_VERSION_RE.match(marimba_version))
    if is_release:
        agent["schema:installUrl"] = f"https://pypi.org/project/marimba/{marimba_version}/"
    # The release version already identifies the code; the git tag would only duplicate it.
    git_info = {**_marimba_git_info(), "url": MARIMBA_REPOSITORY_URL}
    git_info.pop("tag", None)
    if "commit" in git_info:
        agent.update(_git_properties(git_info, _MARIMBA_DIRTY_DESCRIPTION))
    elif is_release:
        agent["schema:url"] = f"{MARIMBA_REPOSITORY_URL}/releases/tag/v{marimba_version}"
    return agent


def build_provenance_document(
    *,
    image_set_uuid: str,
    dataset_name: str,
    dataset_version: str | None,
    pipeline_repos: dict[str, Path],
    packaged_datetime: datetime,
    marimba_version: str,
    started_datetime: datetime | None = None,
) -> dict[str, Any]:
    """
    Build a PROV-O (JSON-LD) provenance document for a packaged dataset.

    Args:
        image_set_uuid: The dataset's image-set UUID (used as the entity identifier and the base of every node IRI).
        dataset_name: The dataset name.
        dataset_version: The dataset version, if any.
        pipeline_repos: Mapping of pipeline name to its source repository directory.
        packaged_datetime: The UTC time at which packaging completed.
        marimba_version: The Marimba version performing the packaging.
        started_datetime: The UTC time at which packaging started, if known.

    Returns:
        A JSON-LD-serialisable dict describing the packaging provenance.
    """
    dataset_iri = f"urn:uuid:{image_set_uuid}"
    activity_id = f"{dataset_iri}#packaging"
    marimba_id = f"{dataset_iri}#marimba"

    dataset_entity: dict[str, Any] = {
        "@id": dataset_iri,
        "@type": ["prov:Entity", "schema:Dataset"],
        "schema:name": dataset_name,
    }
    if dataset_version:
        dataset_entity["schema:version"] = dataset_version
    dataset_entity["prov:generatedAtTime"] = _xsd_datetime(packaged_datetime)
    dataset_entity["prov:wasGeneratedBy"] = {"@id": activity_id}
    dataset_entity["prov:wasAttributedTo"] = {"@id": marimba_id}

    software_agents = [_pipeline_agent(dataset_iri, name, pipeline_repos[name]) for name in sorted(pipeline_repos)]
    software_agents.extend(_tool_agents(dataset_iri))

    activity: dict[str, Any] = {"@id": activity_id, "@type": "prov:Activity"}
    if started_datetime is not None:
        activity["prov:startedAtTime"] = _xsd_datetime(started_datetime)
    activity["prov:endedAtTime"] = _xsd_datetime(packaged_datetime)
    activity["prov:wasAssociatedWith"] = [{"@id": marimba_id}, *({"@id": agent["@id"]} for agent in software_agents)]

    graph = [dataset_entity, activity, _marimba_agent(dataset_iri, marimba_version), *software_agents]
    return {"@context": _PROV_CONTEXT, "@graph": graph}
