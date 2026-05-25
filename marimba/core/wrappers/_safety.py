"""Read-only / hard-link safety checks invoked from DatasetWrapper packaging.

These helpers used to live as private methods on ProjectWrapper. They have no implicit dependency on
ProjectWrapper state beyond the logger and the dataset_mapping argument, so extracting them as free
functions shrinks ProjectWrapper without changing behaviour.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer
from rich.console import Console

from marimba.core.utils.constants import EXIF_SUPPORTED_EXTENSIONS, MAX_SAMPLE_FILES_IN_WARNING
from marimba.core.utils.paths import detect_hardlinked_files, detect_readonly_files
from marimba.core.utils.rich import warning_panel

if TYPE_CHECKING:
    import logging

    from marimba.core import MarimbaError
    from marimba.core.schemas.base import BaseMetadata


DatasetMapping = dict[
    str,
    dict[
        str,
        dict[Path, tuple[Path, list["BaseMetadata"] | None, dict[str, Any] | None]],
    ],
]


def _collect_exif_target_files(dataset_mapping: DatasetMapping) -> list[Path]:
    """Collect every source file in dataset_mapping that EXIF metadata will be written to."""
    files: list[Path] = []
    for pipeline_data in dataset_mapping.values():
        for collection_data in pipeline_data.values():
            for source_path, (_dest_path, metadata_list, _) in collection_data.items():
                if source_path.suffix.lower() in EXIF_SUPPORTED_EXTENSIONS and metadata_list is not None:
                    files.append(source_path)
    return files


def check_hardlinks_and_warn(
    dataset_mapping: DatasetMapping,
    logger: logging.Logger,
) -> None:
    """Warn the user about hard-linked source files and prompt to abort or continue.

    Raises:
        typer.Exit: If the user chooses to abort packaging.
    """
    files_to_check = _collect_exif_target_files(dataset_mapping)
    if not files_to_check:
        return

    hardlinked_files = detect_hardlinked_files(files_to_check)
    if not hardlinked_files:
        return

    file_count = len(hardlinked_files)
    warning_message = (
        f"During packaging, Marimba will destructively modify EXIF data in image files.\n"
        f"The following {file_count} files are hard-linked to external sources and will be modified:\n\n"
    )

    sample_files = hardlinked_files[:MAX_SAMPLE_FILES_IN_WARNING]
    for file_path in sample_files:
        warning_message += f"  {file_path}\n"

    if file_count > MAX_SAMPLE_FILES_IN_WARNING:
        warning_message += f"  ... (showing first {MAX_SAMPLE_FILES_IN_WARNING} of {file_count} files)\n"

    warning_message += "\nThis means your original source files will be permanently modified."

    console = Console()
    console.print(warning_panel(warning_message, title="Hard-linked files detected!"))

    logger.warning(f"Hard-linked files detected during packaging: {file_count} files will be modified")

    prompt_user_for_hard_link_continuation(logger)


def prompt_user_for_hard_link_continuation(logger: logging.Logger) -> None:
    """Prompt user to continue with hard-link packaging or abort."""
    try:
        response = typer.prompt("Continue anyway? [Y/n]", type=str, default="y")
        if response.lower() in ["n", "no"]:
            logger.info("Packaging aborted by user due to hard-link warning")
            raise typer.Exit(code=1) from None
    except (KeyboardInterrupt, EOFError) as exc:
        logger.info("Packaging aborted by user (interrupted)")
        raise typer.Exit(code=1) from exc


def check_readonly_files_and_fail(
    dataset_mapping: DatasetMapping,
    logger: logging.Logger,
    error_cls: type[MarimbaError],
) -> None:
    """Raise ``error_cls`` if any EXIF-target source file in the mapping is read-only.

    Runs before the dry-run gate by design: dry-run is a preview of a future real run; if the real run
    would fail on read-only files, the operator should learn that now.

    Args:
        dataset_mapping: The dataset mapping containing files to be packaged.
        logger: Logger to record the failure summary to.
        error_cls: The exception class to raise. Passed in by the caller so the public exception type
            stays attached to whichever wrapper class owns the contract (typically
            ``ProjectWrapper.ReadOnlyFilesError``).

    Raises:
        error_cls: If any source file is read-only.
    """
    files_to_check = _collect_exif_target_files(dataset_mapping)
    if not files_to_check:
        return

    readonly_files = detect_readonly_files(files_to_check)
    if not readonly_files:
        return

    file_count = len(readonly_files)
    error_message = (
        f"Cannot package dataset: {file_count} files are read-only and cannot be modified.\n"
        f"Marimba requires write permissions to embed EXIF metadata during packaging.\n\n"
        f"The following files need write permissions:\n\n"
    )

    sample_files = readonly_files[:MAX_SAMPLE_FILES_IN_WARNING]
    for file_path in sample_files:
        error_message += f"  {file_path}\n"

    if file_count > MAX_SAMPLE_FILES_IN_WARNING:
        error_message += f"  ... (showing first {MAX_SAMPLE_FILES_IN_WARNING} of {file_count} files)\n"

    error_message += "\nTo fix this issue, run:\n  chmod +w <files>"

    logger.error(f"Packaging failed: {file_count} read-only files detected that cannot be written")

    raise error_cls(error_message)
