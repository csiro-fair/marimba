"""
Marimba provenance record.

Builds a W3C PROV-O provenance document (serialised as JSON-LD) for a packaged Marimba dataset. The packaging run
is modelled as a ``prov:Activity`` that generated the dataset (a ``prov:Entity``) and was associated with the
software agents involved: Marimba itself, each processing pipeline (with its git provenance), and the external
tools (ExifTool, FFmpeg) whose versions materially affect the output.

The record is best-effort: git metadata is read from each pipeline's source repository when present, and tool
versions are queried at packaging time; anything unavailable is simply omitted rather than failing the package.
"""

import contextlib
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from marimba.core.utils.log import get_logger

logger = get_logger(__name__)

MARIMBA_REPOSITORY_URL = "https://github.com/csiro-fair/marimba"

_PROV_CONTEXT: dict[str, str] = {
    "prov": "http://www.w3.org/ns/prov#",
    "schema": "http://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "marimba": "https://github.com/csiro-fair/marimba#",
}


def _is_remote_url(url: str) -> bool:
    """True for a real remote URL (https/ssh/git scheme or scp-style), not a local filesystem path."""
    return "://" in url or bool(re.match(r"^[\w.-]+@[\w.-]+:", url))


def _read_git_info(repo_dir: Path) -> dict[str, str]:
    """Read commit, remote URL, branch, and release tag from a git repository (best-effort)."""
    info: dict[str, str] = {}
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
                url = repo.remotes.origin.url
                if _is_remote_url(url):
                    info["url"] = url
            # Detached HEAD (e.g. a clone checked out at a specific commit) has no active branch.
            with contextlib.suppress(TypeError):
                info["branch"] = repo.active_branch.name
            # Record a release tag only when the commit is exactly on one.
            with contextlib.suppress(GitCommandError):
                info["tag"] = repo.git.describe("--tags", "--exact-match")
        finally:
            repo.close()
    except Exception:
        logger.exception(f"Failed to read git metadata at {repo_dir}")
    return info


def _marimba_git_info() -> dict[str, str]:
    """Best-effort git info for Marimba itself when running from a source checkout (empty if pip-installed)."""
    # marimba/core/utils/provenance.py -> repo root is parents[3].
    return _read_git_info(Path(__file__).resolve().parents[3])


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


_GIT_FIELD_TO_PROPERTY = {
    "url": "schema:codeRepository",
    "commit": "marimba:commit",
    "branch": "marimba:branch",
    "tag": "marimba:tag",
}


def _pipeline_agent(name: str, repo_dir: Path) -> dict[str, Any]:
    """Build a pipeline software agent, enriched with whatever git provenance is available."""
    agent: dict[str, Any] = {"@id": f"#pipeline-{name}", "@type": "prov:SoftwareAgent", "schema:name": name}
    git_info = _read_git_info(repo_dir)
    for field, prop in _GIT_FIELD_TO_PROPERTY.items():
        if field in git_info:
            agent[prop] = git_info[field]
    return agent


def _tool_agents() -> list[dict[str, Any]]:
    """Build software agents for the external tools whose versions are available."""
    agents: list[dict[str, Any]] = []
    exiftool_version = _exiftool_version()
    if exiftool_version:
        agents.append(
            {
                "@id": "#exiftool",
                "@type": "prov:SoftwareAgent",
                "schema:name": "ExifTool",
                "schema:softwareVersion": exiftool_version,
            },
        )
    ffmpeg_version = _ffmpeg_version()
    if ffmpeg_version:
        agents.append(
            {
                "@id": "#ffmpeg",
                "@type": "prov:SoftwareAgent",
                "schema:name": "FFmpeg",
                "schema:softwareVersion": ffmpeg_version,
            },
        )
    return agents


def _marimba_agent(marimba_version: str) -> dict[str, Any]:
    """Build the Marimba software agent, with its own git commit/tag when run from a checkout."""
    agent: dict[str, Any] = {
        "@id": "#marimba",
        "@type": "prov:SoftwareAgent",
        "schema:name": "Marimba",
        "schema:softwareVersion": marimba_version,
        "schema:codeRepository": MARIMBA_REPOSITORY_URL,
    }
    git_info = _marimba_git_info()
    if "commit" in git_info:
        agent["marimba:commit"] = git_info["commit"]
    if "tag" in git_info:
        agent["marimba:tag"] = git_info["tag"]
    return agent


def build_provenance_document(
    *,
    image_set_uuid: str,
    dataset_name: str,
    dataset_version: str | None,
    pipeline_repos: dict[str, Path],
    packaged_datetime: datetime,
    marimba_version: str,
) -> dict[str, Any]:
    """
    Build a PROV-O (JSON-LD) provenance document for a packaged dataset.

    Args:
        image_set_uuid: The dataset's image-set UUID (used as the entity identifier).
        dataset_name: The dataset name.
        dataset_version: The dataset version, if any.
        pipeline_repos: Mapping of pipeline name to its source repository directory.
        packaged_datetime: The UTC time at which packaging completed.
        marimba_version: The Marimba version performing the packaging.

    Returns:
        A JSON-LD-serialisable dict describing the packaging provenance.
    """
    graph: list[dict[str, Any]] = []

    dataset_entity: dict[str, Any] = {
        "@id": f"urn:uuid:{image_set_uuid}",
        "@type": "prov:Entity",
        "schema:name": dataset_name,
        "prov:wasGeneratedBy": {"@id": "#packaging"},
        "prov:wasAttributedTo": {"@id": "#marimba"},
    }
    if dataset_version:
        dataset_entity["schema:version"] = dataset_version
    graph.append(dataset_entity)

    software_agents = [_pipeline_agent(name, pipeline_repos[name]) for name in sorted(pipeline_repos)]
    software_agents.extend(_tool_agents())
    graph.extend(software_agents)
    associated_with = [{"@id": "#marimba"}, *({"@id": agent["@id"]} for agent in software_agents)]

    activity: dict[str, Any] = {
        "@id": "#packaging",
        "@type": "prov:Activity",
        "prov:endedAtTime": {
            "@type": "xsd:dateTime",
            "@value": packaged_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "prov:wasAssociatedWith": associated_with,
    }
    graph.append(activity)

    graph.append(_marimba_agent(marimba_version))

    return {"@context": _PROV_CONTEXT, "@graph": graph}
