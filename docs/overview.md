# Marimba Overview and CLI Usage Guide

Marimba is a Python framework for structuring, managing and processing
[FAIR (Findable, Accessible, Interoperable, Reusable)](https://ardc.edu.au/resource/fair-data/) scientific image
datasets. This guide gives you an architectural understanding of how Marimba is organised and a complete reference for
the command-line interface used to drive data processing workflows. If you want to build your own processing logic, read
the [Pipeline Implementation Guide](pipeline.md) next; if you want to script or automate Marimba, see the
[CLI Scripting Guide](cli.md); and if you want to drive Marimba programmatically from Python, see the
[API Reference](api.md).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
   - [Core Concepts](#core-concepts)
   - [Project Directory Structure](#project-directory-structure)
   - [Processing Workflow](#processing-workflow)
2. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Installation](#installation)
   - [Creating Your First Project](#creating-your-first-project)
3. [CLI Command Reference](#cli-command-reference)
   - [Global Options](#global-options)
   - [`marimba new`](#marimba-new)
   - [`marimba import`](#marimba-import)
   - [`marimba process`](#marimba-process)
   - [`marimba package`](#marimba-package)
   - [`marimba distribute`](#marimba-distribute)
   - [`marimba update`](#marimba-update)
   - [`marimba install`](#marimba-install)
   - [`marimba delete`](#marimba-delete)
   - [`marimba version`](#marimba-version)
4. [Configuration and Metadata](#configuration-and-metadata)
   - [Pipeline Configuration](#pipeline-configuration)
   - [Collection Configuration](#collection-configuration)
   - [Non-Interactive Configuration](#non-interactive-configuration)
   - [Extra Pass-Through Arguments](#extra-pass-through-arguments)
5. [Common Options](#common-options)
   - [Dry-Run Mode](#dry-run-mode)
   - [Parallel Processing](#parallel-processing)
   - [Targeting Pipelines and Collections](#targeting-pipelines-and-collections)
6. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

### Core Concepts

Marimba is organised around five concepts that together take raw instrument data through to a packaged, distributable
dataset.

- **Project**: the top-level workspace. A project is a directory with a standardised layout that holds your installed
  pipelines, imported collections, packaged datasets and distribution targets, along with project-wide configuration and
  logs. Every other concept lives inside a project.
- **Pipeline**: the processing implementation for a specific instrument or multi-instrument system. A pipeline is an
  external Git repository containing a Python class that inherits from `BasePipeline` and implements the import, process
  and package steps. Pipelines are installed into a project and are versioned independently of it. See the
  [Pipeline Implementation Guide](pipeline.md) for how to write one.
- **Collection**: a discrete import of data into the project. Each collection is an isolated unit that pipelines
  process independently and in parallel. Collections carry their own configuration (for example a deployment or site
  identifier) and hold a per-pipeline data directory.
- **Dataset**: the FAIR-compliant output. Packaging assembles processed collection data into a dataset directory
  complete with metadata (iFDO by default), a file-integrity manifest, a provenance record, a human-readable summary and
  a geospatial map.
- **Target**: a distribution endpoint (for example an S3 bucket) that a packaged dataset can be uploaded to.

### Project Directory Structure

`marimba new project` creates a directory with the following layout. Sub-directories are populated as you install
pipelines, import collections and package datasets.

```plaintext
my-project/
├── collections/              # Imported data collections
│   └── my-collection/
│       ├── collection.yml    # Collection configuration
│       └── <pipeline-name>/  # Per-pipeline data directory
├── pipelines/                # Installed pipeline repositories
│   └── my-pipeline/
│       ├── pipeline.yml      # Pipeline configuration
│       ├── repo/             # Cloned pipeline Git repository
│       └── <pipeline>.log    # Pipeline log
├── datasets/                 # Packaged FAIR datasets
│   └── my-dataset/
│       ├── data/             # Dataset files, organised by pipeline
│       │   └── <pipeline>/   # Destination layout returned by the pipeline
│       ├── logs/             # Copied project and pipeline logs
│       ├── pipelines/        # Copied pipeline source
│       ├── ifdo.json         # iFDO metadata (JSON by default; YAML with --metadata-output yaml)
│       ├── summary.md        # Human-readable summary
│       ├── map.png           # Geospatial overview map (when imagery is geolocated)
│       ├── manifest.txt      # File-integrity manifest
│       └── provenance.json   # PROV-O provenance record
├── targets/                  # Distribution target configurations
│   └── my-target.yml
└── project.log               # Project-level log
```

### Processing Workflow

A typical Marimba workflow proceeds through four stages, each backed by a CLI command:

1. **Import** (`marimba import`) - bring raw data into a collection, optionally by copy, move or link.
2. **Process** (`marimba process`) - run each pipeline's processing logic over its collection data in parallel
   sandboxes.
3. **Package** (`marimba package`) - assemble processed data into a FAIR dataset with metadata, manifest and provenance.
4. **Distribute** (`marimba distribute`) - upload a packaged dataset to a configured target.

Project, pipeline and collection scaffolding is created up front with `marimba new`, and pipelines are kept current with
`marimba update` and `marimba install`.

---

## Getting Started

### Prerequisites

- **Python 3.12, 3.13 or 3.14** (`requires-python = ">=3.12,<3.15"`).
- **[ExifTool](https://exiftool.org/)**: required for reading and embedding image metadata. It is validated at the
  start of `import`, `process` and `package`; those commands exit with an actionable error if it is not on your `PATH`.
- **Git**: required for installing and updating pipelines, which are Git repositories.
- **FFmpeg libraries**: required only when a pipeline processes video. Marimba uses [PyAV](https://pyav.org/), which
  bundles or links the FFmpeg libraries; you do not need the standalone `ffmpeg` command-line tool on your `PATH`.

For a full walk-through of setting up a development environment (including installing ExifTool on each platform), see
the [Environment Setup Guide](environment.md).

### Installation

Install the released package from PyPI:

```bash
pip install marimba
```

Verify the installation:

```bash
marimba version
marimba --help
```

To work on Marimba itself, clone the repository and use [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/csiro-fair/marimba.git
cd marimba
uv sync --dev
uv run marimba --help
```

### Creating Your First Project

The following sequence creates a project, installs a pipeline, imports data, processes it and packages a dataset.
Replace the pipeline URL and data path with your own.

```bash
# 1. Create and enter a new project
marimba new project my-first-project
cd my-first-project

# 2. Install a pipeline from its Git repository
marimba new pipeline my-pipeline https://github.com/org/my-pipeline.git

# 3. Import raw data into a collection
marimba import my-collection /path/to/raw/data

# 4. Process all collections with all pipelines
marimba process

# 5. Package the processed data into a FAIR dataset
marimba package my-dataset \
  --contact-name "Your Name" \
  --contact-email "you@example.org"
```

---

## CLI Command Reference

All commands are invoked as `marimba <command>`. Run `marimba <command> --help` (or `marimba <command> <subcommand>
--help`) at any time for the authoritative, version-specific option list. Every command that mutates state supports
`--dry-run`; see [Dry-Run Mode](#dry-run-mode).

Commands that operate on an existing project locate the project root automatically by searching the current working
directory and its parents. Pass `--project-dir PATH` to point at a specific project instead.

### Global Options

These options apply to the top-level `marimba` command and must appear before the subcommand.

| Option | Description |
| --- | --- |
| `--level [DEBUG\|INFO\|WARNING\|ERROR]` | Logging verbosity written to the terminal. Default: `WARNING`. |
| `--version` | Show the Marimba version and exit. |
| `--install-completion` | Install shell completion for the current shell. |
| `--show-completion` | Print shell completion so you can copy or customise it. |
| `--help` | Show help and exit. |

```bash
# Raise verbosity for a single command
marimba --level INFO process
```

### `marimba new`

Create a new project, pipeline, collection or target.

#### `marimba new project PROJECT_DIR`

Creates a new project at `PROJECT_DIR`.

```bash
marimba new project /data/marine-survey-2026
```

#### `marimba new pipeline PIPELINE_NAME URL`

Clones the pipeline Git repository at `URL`, installs it under the given `PIPELINE_NAME`, and prompts for any
pipeline-level configuration the pipeline declares.

| Option | Description |
| --- | --- |
| `--config TEXT` | YAML/JSON config file path, or a JSON object string, merged with the prompted configuration. |
| `--accept-defaults`, `-y` | Accept all default configuration values without prompting. |
| `--project-dir PATH` | Project root to operate on. |

```bash
marimba new pipeline benthic-camera https://github.com/org/benthic-camera-pipeline.git \
  --config '{"platform_id": "CAM01"}'
```

#### `marimba new collection COLLECTION_NAME [PARENT_COLLECTION_NAME]`

Creates an empty collection and prompts for its configuration. An optional parent collection name lets the new
collection inherit configuration; if omitted, the most recently modified collection is used as the parent.

| Option | Description |
| --- | --- |
| `--config TEXT` | YAML/JSON config file path, or a JSON object string, merged with the prompted configuration. |
| `--accept-defaults`, `-y` | Accept all default configuration values without prompting. |
| `--project-dir PATH` | Project root to operate on. |

```bash
marimba new collection dive-001 --config '{"site_id": "GBR-001"}'
```

> In most workflows you do not need `marimba new collection`: `marimba import` creates the target collection on demand.
> Use it when you want to create and configure a collection before importing into it.

#### `marimba new target TARGET_NAME`

Creates a distribution target and prompts for its type and connection details (for example an S3 bucket, endpoint and
credentials).

| Option | Description |
| --- | --- |
| `--project-dir PATH` | Project root to operate on. |

```bash
marimba new target s3-public
```

### `marimba import`

```
marimba import COLLECTION_NAME SOURCE_PATHS...
```

Imports one or more source files or directories into a collection, creating the collection if it does not yet exist.
Each installed pipeline's `_import` logic decides how the source data is brought in.

| Argument | Description |
| --- | --- |
| `COLLECTION_NAME` | Target collection name. |
| `SOURCE_PATHS...` | One or more source files or directories to import. |

| Option | Description |
| --- | --- |
| `--parent-collection-name TEXT` | Parent collection to inherit configuration from. Defaults to the last collection. |
| `--pipeline-name TEXT` | Limit the import to specific pipelines (repeatable). Defaults to all pipelines. |
| `--operation [copy\|move\|link]` | How source files are brought in. Default: `copy`. |
| `--overwrite` / `--no-overwrite` | Overwrite an existing collection of the same name. Default: `--no-overwrite`. |
| `--config TEXT` | YAML/JSON config file path, or a JSON object string. |
| `--accept-defaults`, `-y` | Accept all default configuration values without prompting. |
| `--extra TEXT` | Extra key-value pass-through argument (repeatable). |
| `--dry-run` / `--no-dry-run` | Preview without changing files. Default: `--no-dry-run`. |
| `--max-workers INTEGER` | Maximum worker processes. Default: all available CPU cores. |
| `--project-dir PATH` | Project root to operate on. |

```bash
# Import a directory, linking files instead of copying
marimba import dive-001 /media/sd-card/ --operation link

# Import from multiple sources into one collection
marimba import combined /data/source-a /data/source-b /data/source-c
```

### `marimba process`

```
marimba process
```

Runs each pipeline's `_process` logic over its collection data. With no targeting options, all collections are processed
by all pipelines, in parallel.

| Option | Description |
| --- | --- |
| `--collection-name TEXT` | Limit to specific collections (repeatable). Defaults to all collections. |
| `--pipeline-name TEXT` | Limit to specific pipelines (repeatable). Defaults to all pipelines. |
| `--extra TEXT` | Extra key-value pass-through argument (repeatable). |
| `--dry-run` / `--no-dry-run` | Preview without changing files. Default: `--no-dry-run`. |
| `--max-workers INTEGER` | Maximum worker processes. Default: all available CPU cores. |
| `--project-dir PATH` | Project root to operate on. |

```bash
# Process one collection with one pipeline
marimba process --collection-name dive-001 --pipeline-name benthic-camera
```

### `marimba package`

```
marimba package DATASET_NAME
```

Assembles processed collection data into a FAIR dataset directory: it populates the data files, generates metadata
(iFDO by default), builds a file-integrity manifest, writes a provenance record, renders a summary and, where geolocated
imagery is present, a map.

| Argument | Description |
| --- | --- |
| `DATASET_NAME` | Name for the packaged dataset. |

| Option | Description |
| --- | --- |
| `--collection-name TEXT` | Limit to specific collections (repeatable). Defaults to all collections. |
| `--pipeline-name TEXT` | Limit to specific pipelines (repeatable). Defaults to all pipelines. |
| `--operation [copy\|move\|link]` | How dataset files are populated. Default: `copy`. |
| `--version TEXT` | Dataset version. Default: `1.0`. |
| `--contact-name TEXT` | Full name of the dataset contact person. |
| `--contact-email TEXT` | Email address of the dataset contact person. |
| `--zoom INTEGER` | Zoom level for the generated dataset map. |
| `--metadata-output [json\|yaml]` | Metadata serialisation format. Default: `json`. |
| `--metadata-level [project\|pipeline\|collection]` | Granularity at which metadata is grouped and written. |
| `--exif-chunk-size INTEGER` | Chunk size for EXIF processing. Defaults to adaptive sizing. |
| `--force` / `--no-force` | Package without prompting on hard-link warnings. Default: `--no-force`. |
| `--allow-destination-collisions` / `--no-allow-destination-collisions` | Allow multiple sources to map to the same destination; the first source wins. Default: `--no-allow-destination-collisions`. |
| `--extra TEXT` | Extra key-value pass-through argument (repeatable). |
| `--dry-run` / `--no-dry-run` | Preview without changing files. Default: `--no-dry-run`. |
| `--max-workers INTEGER` | Maximum worker processes. Default: all available CPU cores. |
| `--project-dir PATH` | Project root to operate on. |

```bash
marimba package marine-survey-2026 \
  --version 2.1 \
  --contact-name "Dr Marine Scientist" \
  --contact-email "scientist@institution.org" \
  --zoom 12
```

### `marimba distribute`

```
marimba distribute DATASET_NAME TARGET_NAME
```

Uploads a packaged dataset to a configured distribution target.

| Argument | Description |
| --- | --- |
| `DATASET_NAME` | Dataset to distribute. |
| `TARGET_NAME` | Distribution target to upload to. |

| Option | Description |
| --- | --- |
| `--validate` / `--no-validate` | Validate the dataset against its manifest before distribution. Default: `--validate`. |
| `--dry-run` / `--no-dry-run` | Preview without uploading. Default: `--no-dry-run`. |
| `--project-dir PATH` | Project root to operate on. |

```bash
marimba distribute marine-survey-2026 s3-public
```

### `marimba update`

```
marimba update
```

Pulls the latest changes for every installed pipeline repository.

| Option | Description |
| --- | --- |
| `--project-dir PATH` | Project root to operate on. |

### `marimba install`

```
marimba install
```

Installs the Python dependencies declared in each pipeline's `requirements.txt`.

| Option | Description |
| --- | --- |
| `--project-dir PATH` | Project root to operate on. |

### `marimba delete`

Delete a project, pipeline, collection, dataset or target. Every `delete` subcommand accepts `--project-dir PATH` and
`--dry-run` / `--no-dry-run` (default `--no-dry-run`).

```
marimba delete project
marimba delete pipeline PIPELINE_NAME
marimba delete collection COLLECTION_NAME
marimba delete dataset DATASET_NAME
marimba delete target TARGET_NAME
```

```bash
# Preview what deleting a collection would remove
marimba delete collection dive-001 --dry-run
```

### `marimba version`

```
marimba version
```

Prints the installed Marimba version. Equivalent to `marimba --version`.

---

## Configuration and Metadata

Marimba collects configuration at two levels, both defined by the pipeline and both stored as YAML inside the project.
The set of fields prompted for is entirely determined by the pipeline you install - see
[Implementing Your Pipeline](pipeline.md) for how these schemas are declared.

### Pipeline Configuration

Pipeline-level configuration is declared by a pipeline's `get_pipeline_config_schema()` method and stored in
`pipelines/<pipeline-name>/pipeline.yml`. It captures values that are constant across every collection the pipeline
processes - for example a platform identifier, instrument specification or principal-investigator name. You are prompted
for it when you run `marimba new pipeline`.

### Collection Configuration

Collection-level configuration is declared by a pipeline's `get_collection_config_schema()` method and stored in
`collections/<collection-name>/collection.yml`. It captures values specific to a single import - for example a
deployment or site identifier, or a collection date. You are prompted for it when you run `marimba import` (or
`marimba new collection`).

### Non-Interactive Configuration

Both `--config` and `--accept-defaults` let you avoid interactive prompts, which is essential for scripting:

- `--config collection.yml` supplies configuration from a YAML or JSON file (or an inline JSON object string); values
  are merged with the schema defaults, and any remaining unset fields are still prompted for unless you also pass
  `--accept-defaults`.
- `--accept-defaults` (`-y`) accepts every default value without prompting.

See the [CLI Scripting Guide](cli.md) for non-interactive automation patterns.

### Extra Pass-Through Arguments

`--extra key=value` passes arbitrary key-value arguments through to the pipeline's `_import`, `_process` and `_package`
methods, where they are available as keyword arguments. Repeat the flag to pass several:

```bash
marimba process --extra thumbnail_size=512 --extra quality=high
```

---

## Common Options

### Dry-Run Mode

`--dry-run` is supported on every command that mutates state (`import`, `process`, `package`, `distribute` and each
`delete` subcommand). It executes the command and prints the log of what *would* happen, prefixed with `DRY_RUN -`, but
performs no filesystem changes. Structural and safety validation (project/collection layout checks, dataset-mapping
checks, read-only and hard-link detection, dependency validation) still run, so a dry run surfaces problems a real run
would hit.

```bash
marimba package test-dataset --dry-run
```

### Parallel Processing

Marimba parallelises import, processing and packaging across collections. By default it uses all available CPU cores;
cap concurrency with `--max-workers` - useful on shared or memory-constrained systems:

```bash
marimba process --max-workers 4
```

### Targeting Pipelines and Collections

`--pipeline-name` and `--collection-name` are repeatable and combine to target any subset of the work. With neither, a
command applies to all pipelines and all collections.

```bash
# Two pipelines across two collections
marimba process \
  --pipeline-name camera --pipeline-name sensor \
  --collection-name dive-001 --collection-name dive-002
```

---

## Troubleshooting

- **`ExifTool is not available`**: install ExifTool and ensure it is on your `PATH`. See the
  [Environment Setup Guide](environment.md).
- **A pipeline fails to import or process**: run `marimba install` to install its dependencies, then re-run with
  `--level DEBUG` to see per-pipeline detail.
- **Packaging reports destination collisions**: two source files resolve to the same destination path. Fix the
  pipeline's `_package` mapping, or pass `--allow-destination-collisions` to keep the first source.
- **Packaging pauses on a hard-link warning**: pass `--force` to proceed non-interactively, or re-import with a
  different `--operation`.
- **Preview before committing**: run any mutating command with `--dry-run` first to see exactly what it would do.

For developing custom pipelines, see the [Pipeline Implementation Guide](pipeline.md). For scripting and automation, see
the [CLI Scripting Guide](cli.md). For programmatic use, see the [API Reference](api.md).
