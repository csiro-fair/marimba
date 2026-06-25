# Changelog

All notable changes to Marimba are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.2.0] – 2026-06-25

### Added
- W3C PROV-O `provenance.json` record written to every packaged dataset, capturing the pipeline git commit and repository URL, the Marimba and ExifTool/FFmpeg versions, the dataset version, the packaging time, and a deterministic image-curation-protocol summary.
- Deterministic `image-set-uuid` derived from the dataset name, so re-packaging the same input produces a stable dataset identifier instead of a random one.
- Deterministic per-image `image-uuid` derived from the set UUID and the image's dataset-relative path, minted when a pipeline leaves it unset.
- iFDO header spatial bounding box (`image-set-min/max-latitude/longitude-degrees`) for spatial discovery.
- Discrete rights, attribution, and identity metadata embedded as standard EXIF and XMP fields: creators to `Artist`/`dc:Creator`, ORCID to `plus:ImageCreator`, licence and copyright to the `xmpRights` block, and the image UUID to `dc:Identifier`.
- UTC GPS fix-time tags (`GPSDateStamp`/`GPSTimeStamp`) and a `GPSMapDatum` tag honouring the pipeline-supplied coordinate reference system on geotagged imagery.
- Dataset identity (`image-set-uuid`, `image-set-name`, `hash-algorithm`) recorded as header lines in `manifest.txt` and the dataset summary, with manifest parsing made robust to `:` in values.
- Marimba and iFDO paper citations injected into each dataset's `image-set-related-material` by default; pipeline-supplied entries merge ahead of the defaults and are keyed by URI.
- iFDO completeness check that reports schema-required iFDO fields left unpopulated, in the log and as an end-of-packaging summary panel; it never fails packaging or changes the output.

### Changed
- Generated iFDO files now target schema version v2.2.1 (previously v2.1.0), including normalising `image-entropy` to the [0, 1] range the schema requires.
- Dataset metadata (iFDO and generic) is now serialised as JSON by default instead of YAML, which is dramatically faster on large datasets (tens of seconds of YAML serialisation reduced to under a second for multi-hundred-thousand-image sets). Pass `--metadata-output yaml` to `package` to keep the previous YAML output.
- FFmpeg is no longer a required system dependency; video summaries and corruption checks now run through PyAV (already a hard dependency) instead of shelling out to the `ffprobe` and `ffmpeg` CLIs.
- Image transforms preserve source EXIF and ICC metadata through both the PIL transforms and the OpenCV filters (`apply_clahe`, `gaussian_blur`, `sharpen`); the orientation tag is reset only when an image is rotated or flipped.
- Process pools now pin an explicit `forkserver` start method (falling back to `spawn` where unavailable, e.g. Windows) instead of using the platform default. This avoids the deadlock hazard of forking a multithreaded process, and keeps concurrency behaviour consistent across Python versions rather than silently switching from `fork` to `forkserver` on Python 3.14.
- `BaseMetadata.create_dataset_metadata` now takes a required `logger` argument; pipelines that subclass `BaseMetadata` and override this method must add the parameter.
- `marimba/lib/image.py` returns per-channel averages for any image mode.

### Fixed
- Dataset map rendering is now best-effort: an unreachable or blocked OpenStreetMap tile server logs a warning and skips `map.png` rather than aborting the packaging run. Tile fetches use the canonical HTTPS host, an identifiable User-Agent, and a request timeout so they fail fast and stay within OpenStreetMap's usage policy.
- iFDO auto-deduplication no longer mis-runs on single-image datasets.
- iFDO auto-deduplication no longer treats `None` as a value common to all items.
- The iFDO record embedded in each image's EXIF UserComment now serialises `image-datetime` with the dataset's configured `image-datetime-format`, matching the top-level `ifdo.json`. Previously a pipeline that set a custom datetime format produced an embedded record that disagreed with `ifdo.json` and declared one format while storing a value in another.

### Dependencies
- `ifdo` lower bound raised to `>=1.6.0` for `image-set-related-material` support.
- `av` (PyAV) upper bound widened to `<18`; `numpy` upper bound widened to `<2.6`.
- `tabulate`, `typer`, and `rich` upper bounds widened.
- FFmpeg system dependency removed.

## [1.1.0] – 2026-05-27

### Added
- PyExifTool integration replacing `piexif` for all EXIF metadata writing, with support for embedded thumbnails and a much wider range of image formats.
- Memory-aware batch processing that samples actual files to estimate per-image memory requirements and calculates a safe chunk size from available system RAM; adaptive retry logic halves the batch size on memory pressure, preventing OOM crashes on large high-resolution datasets.
- iFDO auto-deduplication automatically promotes fields that are identical across all images in a collection (sensor, platform, deployment event, license, etc.) to the `image-set-header`, reducing output file size.
- `--accept-defaults` flag on the `package` command for fully non-interactive runs suitable for CI/CD pipelines and scripted workflows.
- System dependency checker providing platform-specific installation instructions for required system tools (ExifTool, FFmpeg) when they are missing, replacing generic error messages.
- Marimba version recorded in dataset summary files for provenance tracking.
- Comprehensive test suite: nearly 1,000 tests at 80%+ coverage with unit, integration, and end-to-end tests across every major module.
- Curated public API on the top-level `marimba` package (`BasePipeline`, `BaseMetadata`, `GenericMetadata`, `iFDOMetadata`, `Operation`); lazy submodule loading in `marimba.lib` so `import marimba.lib` is cheap.
- `MarimbaError` exception base class so CLI handlers can catch the broad case while typed `except` for specific failure modes stay narrow.
- CI and nightly GitHub Actions workflows, Dependabot coverage for pip and GitHub Actions, tracked `uv.lock` for reproducible dev and CI environments, and a tiered end-to-end regression harness against the `mritc-demo` dataset that runs both as a pre-push gate and on every CI run.

### Changed
- `image-datetime` values now serialised using the iFDO spec-defined format (`%Y-%m-%d %H:%M:%S.%f`) instead of ISO 8601; existing files with ISO 8601 datetime strings continue to load without error.
- Packaging now detects read-only files before starting and fails immediately with a clear error rather than partway through a long run.
- Hard-linked files trigger a warning prompt before packaging proceeds, since EXIF writes propagate to all linked paths.
- `PipExecutor` replaced by `UvExecutor`; pipelines must be managed through `uv`.
- Python 3.12 or later is now required (previously 3.10+).
- Repository-wide performance pass: lazy imports of heavy native dependencies roughly halve cold CLI startup; a batched ExifTool session shared per pipeline run accelerates EXIF metadata writing; per-file dataset summarisation and S3 distribution uploads are parallelised; dataset-tree walks consolidated into a single pass; pipeline modules cached on first import within a process.
- CLI failures now exit with non-zero status codes (previously exited 0 on errors).
- `marimba update` aggregates per-pipeline failures into a single raised error rather than swallowing them.
- `Manifest.hashes` now keyed by `str` (POSIX-style relative path) rather than `pathlib.Path` for lower memory footprint during packaging.
- `@multithreaded` decorator is fail-fast by default: the first worker exception is re-raised rather than swallowed.

### Removed
- `piexif` dependency removed; replaced by `PyExifTool`.
- `PipExecutor` class removed.

### Dependencies
- `pyexiftool` added.
- `piexif` removed.
- Minimum Python version raised to 3.12.

## [1.0.1] – 2025-06-20

### Fixed
- Empty file-type entries (null or blank values) are now filtered out when generating lists of unique file types in dataset summaries.

### Changed
- Classifiers, project URLs, and license metadata added to `pyproject.toml` for improved PyPI presentation.
- `hatch` and `twine` added to dev dependencies to streamline packaging and distribution.

### Dependencies
- `ifdo` upper bound tightened from `<2` to `<1.3` to ensure compatibility.

## [1.0.0] – 2025-05-16

### Added
- New `marimba/core/schemas/` module with an abstract `BaseMetadata` class and concrete implementations for iFDO, Darwin Core, and Generic metadata schemas, replacing ad-hoc metadata handling in the dataset wrapper (PR #6).
- CLI option to set the metadata generation level (project, collection, or pipeline), giving fine-grained control over where metadata files are written (PR #12).
- CLI option to choose the metadata output format (YAML or JSON); YAML is now the default (PR #8).
- Post-package hook support for pipelines, allowing custom processing steps to run immediately after dataset packaging (PR #15).
- `marimba/core/utils/hash.py` utility module for SHA-256 hash computation with an optional root directory parameter.
- `marimba/core/utils/dataset.py` and `marimba/core/utils/metadata.py` utility modules extracted from the dataset wrapper.
- EXIF identifier injection and improved `UserComment` field encoding.
- Depth extent fields added to image and video metadata summaries.
- Dynamic map zoom calculation and improved axis rendering.
- `py.typed` marker file (PEP 561) so downstream packages can benefit from Marimba's type annotations (PR #17).
- Tests for iFDO schema, dataset utilities, manifest utilities, and the `py.typed` marker.

### Changed
- `opencv-python` replaced by `opencv-python-headless`, removing unnecessary GUI dependencies (PR #10).
- Metadata saving switched from JSON to YAML by default.
- Dataset summary logging now consistently uses `project_wrapper.logger`.
- Logging level raised from `debug` to `info` for key operations across core modules.
- GPS coordinate precision refined in EXIF metadata output.
- JPEG thumbnails set to non-progressive mode.
- Hash values changed from raw bytes to hexadecimal strings throughout.
- Dataset contact-info extraction logic refactored.
- Manifest handling refactored for improved error management, including correct handling of directory hashes and file deletions.
- Project management migrated from Poetry to UV; `pyproject.toml` updated to PEP 517 standard format with `hatchling` as the build backend (PR #18).

### Fixed
- Wrong file path in iFDO checksum computation (PR #16).
- iFDO import handling for edge cases (PR #11).
- PyAV package dependency specification corrected (PR #20).

### Dependencies
- Project management and build backend migrated from Poetry/`poetry-core` to UV/`hatchling`.
- `ifdo` updated to `>=1.2.5,<2`.
- `pillow` updated to `>=11.1.0,<12`.
- `typer` updated to `>=0.15.1,<0.16`.
- `av` (formerly `pyav`) updated to `>=14.0.1,<15`.
- `opencv-python` replaced by `opencv-python-headless`.

## [0.4.8] – 2024-12-16

### Added
- Handling for creating new empty pipelines that have no pre-existing data.

### Fixed
- GPS coordinate precision corrected by updating the EXIF rational denominator to 1000.

## [0.4.7] – 2024-12-03

### Added
- `max_workers` parameter on key processing methods to give callers explicit control over the number of worker threads.
- Logging added to manifest and dataset methods for improved observability.

## [0.4.6] – 2024-11-18

### Fixed
- Operation argument type bug in the pipeline CLI: the `operation` value is now correctly merged into keyword arguments rather than passed as a positional parameter.

### Dependencies
- `ifdo` updated to 1.2.1.

## [0.4.5] – 2024-11-16

### Added
- `multithreaded_generate_video_thumbnails` function for concurrent video thumbnail generation.

### Changed
- Image module refactored for improved grid creation and output path management.
- Image data handling and related dependency versions updated.

## [0.4.4] – 2024-11-13

### Added
- Image grid splitting by maximum height, enabling large images to be divided into fixed-height rows.
- Ruff linter introduced with `.ruff.toml` configuration (PR #3).
- `delete` CLI commands enhanced to support batch operations across multiple targets.

### Changed
- Logging statements removed from the image module to reduce output noise.
- Error handling improved across multiple modules.
- Type hints updated and tightened throughout the codebase.

### Fixed
- `TypeError` raised when identifying the pipeline class is now caught and handled gracefully.

## [0.4.3] – 2024-10-28

### Added
- Coordinate validation methods for geolocation data.

## [0.4.2] – 2024-10-01

No functional changes. Project logo URL updated in documentation.

## [0.4.1] – 2024-09-27

### Added
- Parallel processing for pipeline execution, significantly improving throughput for multi-pipeline workflows (PR #15).
- `delete` command for removing projects, collections, and pipelines.
- Targeted execution: individual pipelines and collections can now be selected when running the `process` and `package` commands (PR #19).
- Hard-link support in the `package` command to avoid duplicating files on disk.
- Video thumbnail generation added to the image processing library.
- Grid and coordinate labelling on summary maps.
- Zoom control for map generation.
- `marimba/lib/concurrency.py` providing a reusable multithreaded decorator with configurable worker count.
- Support for merging user-provided CLI configuration into pipeline defaults.
- User and developer documentation: `docs/overview.md` and `docs/pipeline.md` (PR #1, PR #2).

### Changed
- Main CLI entry point renamed from `marimba.marimba:marimba` to `marimba.main:marimba_cli`.
- Dataset metadata filename updated.
- Error handling in the project wrapper refactored.
- Source path existence is verified before beginning an import operation.

### Removed
- MkDocs documentation build removed from the repository.

### Dependencies
- `tabulate` added.
- `pyav` added for video processing.
- `distlib` added.
- `python-dateutil`, `scikit-learn`, `pyexiv2`, `psutil`, `python-dotenv` removed.
- `mkdocs`, `mkdocstrings`, `mkdocs-material` removed from dev dependencies.
- `pandas` updated from 1.5.3 to 2.2.2.
- `pillow` updated for security.
- `types-tabulate`, `types-requests` added to dev dependencies.

## [0.4.0] – 2024-06-08

### Added
- Multithreaded dataset packaging and multithreaded thumbnail generation.
- Multiprocessing support for pipeline `run_command` execution.
- Reusable multithreaded decorator in the standard library with configurable worker count.
- Parallel pipeline import.
- SHA-256 hash computation for images in iFDO output.
- Progress bars throughout the dataset packaging workflow.
- Image entropy and average colour calculation functions in the standard library.
- `OrderedDict` sorting of `image_set_items` for deterministic iFDO output ordering.
- Comprehensive type annotations across the codebase, validated with MyPy.
- Detailed module-level docstrings throughout.
- Bandit security scan configuration and additional unit tests.

### Changed
- Configuration files moved to a dedicated `config/` directory.
- `NameError` exception renamed to `InvalidNameError` for clarity.
- Dataset and project wrappers updated to include pipeline metadata.
- Metadata handling and iFDO generation methods refactored.
- `opt_metadata` renamed to `ancillary` in the dataset wrapper.
- `image_context` property updated to use `name` instead of `uri`.

### Dependencies
- Minimum Python version raised to 3.10.
- `pillow` updated to 10.3.0 (security).
- `ifdo` updated to 1.1.3.
- `czifile`, `stitching`, `pydoit` removed.
- `python-dotenv` added.
- Dev toolchain extended with `flake8`, `mypy`, `bandit`, `pydocstyle`, `pylint`, `pytest-cov`, `pep8-naming`, `flake8-bugbear`, `flake8-comprehensions`, and `flake8-builtins`.

## [0.3.0] – 2023-11-03

v0.3.0 substantially expands the initial prototype across over 130 files, establishing the core project/collection/pipeline/dataset/target architecture. Distribution targets for AWS S3 and DAP servers are introduced alongside `gitpython` and `boto3` for pipeline repository management and cloud upload. `staticmap` and `piexif` are integrated for geographic summary map generation and EXIF metadata writing. A pre-commit hook configuration, a `pytest` test suite, and MkDocs documentation scaffolding are also included.

### Dependencies
- `gitpython`, `boto3`, `staticmap`, `piexif`, `psutil`, `pydoit` added.
- `cookiecutter` removed.
- `ifdo` updated to 1.1.2.

## [0.2.0] – 2023-09-05

Initial public release. Marimba is a Marine Imagery Batch Actions tool targeting Python 3.8. The release establishes the project layout, a Typer/Rich CLI skeleton, and early integration with the iFDO standard (`ifdo ^1.1.0`). Core dependencies include Pillow for image handling, OpenCV for image processing, `pyexiv2` for EXIF manipulation, `scikit-learn` for data analysis utilities, `pandas` and `czifile` for data ingestion, and a `cookiecutter` template system for pipeline scaffolding.
