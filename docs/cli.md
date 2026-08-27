# Marimba CLI Scripting Guide

This guide shows how to drive Marimba non-interactively so that whole processing workflows can be captured in a script
and reproduced. It focuses on the patterns that make automation reliable - suppressing interactive prompts, handling
exit codes, controlling logging and parallelism, and previewing with dry runs - rather than repeating the per-command
option reference. For the full list of commands and options, see the
[Overview and CLI Usage Guide](overview.md); for writing the pipelines these commands drive, see the
[Pipeline Implementation Guide](pipeline.md).

---

## Table of Contents

1. [Running Marimba Non-Interactively](#running-marimba-non-interactively)
2. [Exit Codes and Error Handling](#exit-codes-and-error-handling)
3. [Logging in Scripts](#logging-in-scripts)
4. [A Complete Workflow Script](#a-complete-workflow-script)
5. [Batching Over Collections](#batching-over-collections)
6. [Previewing with Dry Runs](#previewing-with-dry-runs)
7. [Controlling Parallelism](#controlling-parallelism)
8. [Running on HPC and Shared Systems](#running-on-hpc-and-shared-systems)
9. [Best Practices](#best-practices)

---

## Running Marimba Non-Interactively

By default `marimba new pipeline`, `marimba new collection` and `marimba import` prompt for pipeline- and
collection-level configuration. Scripts must suppress those prompts, using one or both of:

- **`--config path/to/config.yml`**: supply configuration from a YAML or JSON file, or as an inline JSON object
  string. File contents are loaded with `yaml.safe_load` (JSON is valid YAML). They are merged with the pipeline's
  schema defaults. The file is not executed as Python.
- **`--accept-defaults` (`-y`)**: accept every remaining default without prompting.

Supply `--config` for the values you care about and add `--accept-defaults` to accept the rest, so no prompt can block
the script:

```bash
marimba import dive-001 /data/raw/dive-001 \
  --config collection.yml \
  --accept-defaults
```

Inline JSON still works:

```bash
marimba import dive-001 /data/raw/dive-001 \
  --config '{"site_id": "GBR-001"}' \
  --accept-defaults
```

---

## Exit Codes and Error Handling

Marimba commands exit `0` on success and `1` on failure, so standard shell error handling works. Start scripts with
`set -euo pipefail` so a failed step aborts the run instead of silently continuing:

```bash
#!/usr/bin/env bash
set -euo pipefail

marimba process
marimba package my-dataset --contact-name "Your Name" --contact-email "you@example.org"
```

To react to a failure rather than abort, test the exit code explicitly:

```bash
if ! marimba process --collection-name dive-001; then
  echo "Processing failed for dive-001" >&2
  exit 1
fi
```

---

## Logging in Scripts

The global `--level` flag controls terminal verbosity and must appear before the subcommand. Use `DEBUG` while
developing a script and `INFO` for routine runs; the default is `WARNING`.

```bash
marimba --level DEBUG process
```

Marimba also writes a persistent audit trail to `project.log` at the project root (and per-pipeline logs under
`pipelines/<name>/`), independent of `--level`. Timestamp your script's own progress messages to correlate them with the
log:

```bash
echo "[$(date -Is)] starting process"
marimba --level INFO process
echo "[$(date -Is)] process complete"
```

---

## A Complete Workflow Script

A self-contained script that creates a project, installs a pipeline, imports and processes data, and packages a dataset.
It is idempotent enough to re-run by pointing `--project-dir` at a fresh location.

```bash
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/data/projects/marine-survey-2026"
PIPELINE_URL="https://github.com/org/benthic-camera-pipeline.git"
SOURCE_DATA="/data/raw/marine-survey-2026"

# 1. Create the project (skip if it already exists)
if [[ ! -d "$PROJECT_DIR" ]]; then
  marimba new project "$PROJECT_DIR"
fi

# 2. Install the pipeline, accepting default pipeline configuration
marimba new pipeline benthic-camera "$PIPELINE_URL" \
  --project-dir "$PROJECT_DIR" \
  --config '{"platform_id": "CAM01"}' \
  --accept-defaults

# 3. Import the source data into a collection
marimba import survey-2026 "$SOURCE_DATA" \
  --project-dir "$PROJECT_DIR" \
  --operation link \
  --config '{"site_id": "GBR-001"}' \
  --accept-defaults

# 4. Process and package
marimba --level INFO process --project-dir "$PROJECT_DIR"
marimba --level INFO package marine-survey-2026 \
  --project-dir "$PROJECT_DIR" \
  --version 1.0 \
  --contact-name "Dr Marine Scientist" \
  --contact-email "scientist@institution.org"
```

---

## Batching Over Collections

Loop to import several collections from a directory of sources, deriving each collection name from its directory:

```bash
#!/usr/bin/env bash
set -euo pipefail

SOURCE_ROOT="/data/raw/2026"

for src in "$SOURCE_ROOT"/*/; do
  collection="$(basename "$src")"
  marimba import "$collection" "$src" --accept-defaults
done

# Process and package everything at once
marimba process
marimba package survey-2026 --contact-name "Your Name" --contact-email "you@example.org"
```

Import creates each collection on demand, so a single `marimba process` afterwards covers every collection the loop
imported. To process each collection as it is imported instead, move a `marimba process --collection-name
"$collection"` call inside the loop.

---

## Previewing with Dry Runs

Every mutating command supports `--dry-run`, which logs what would happen (prefixed with `DRY_RUN -`) without touching
files, while still running structural and safety validation. A useful scripting pattern is to preview, then run for real
once the preview looks right:

```bash
# Preview
marimba package survey-2026 --contact-name "Your Name" --contact-email "you@example.org" --dry-run

# Run for real
marimba package survey-2026 --contact-name "Your Name" --contact-email "you@example.org"
```

Because a dry run exits non-zero if validation fails, you can gate the real run on a successful preview:

```bash
marimba package survey-2026 --contact-name "Your Name" --contact-email "you@example.org" --dry-run \
  && marimba package survey-2026 --contact-name "Your Name" --contact-email "you@example.org"
```

---

## Controlling Parallelism

Import, process and package parallelise across collections using all available CPU cores by default. On shared or
memory-constrained machines, cap concurrency with `--max-workers` so Marimba does not contend with other work:

```bash
marimba process --max-workers 4
marimba package survey-2026 --max-workers 4 \
  --contact-name "Your Name" --contact-email "you@example.org"
```

Marimba does not read any environment variables to configure runtime behaviour - parallelism is controlled entirely by
`--max-workers`.

---

## Running on HPC and Shared Systems

On a cluster, make the [ExifTool](https://exiftool.org/) dependency available before invoking Marimba - for example via
an environment module if your site provides one - and match `--max-workers` to the cores your job requested rather than
letting Marimba grab every core on the node:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Make ExifTool available (site-specific; only if provided as a module)
module load exiftool 2>/dev/null || true

# Match the scheduler's core allocation
WORKERS="${SLURM_CPUS_PER_TASK:-4}"

marimba --level INFO process --project-dir "$PROJECT_DIR" --max-workers "$WORKERS"
```

Only ExifTool is validated at startup; the FFmpeg libraries used for video are provided by the PyAV Python package, so
you do not need to load a separate `ffmpeg` module. When importing large volumes, prefer `--operation link` to avoid
duplicating data on scratch storage.

---

## Best Practices

1. **Fail fast**: begin scripts with `set -euo pipefail` so a failed step stops the run.
2. **Suppress prompts**: always pair `--config` with `--accept-defaults` in unattended runs.
3. **Define paths once**: hoist project, source and pipeline paths into variables at the top of the script.
4. **Preview first**: run mutating commands with `--dry-run` before the real invocation, especially with
   `--operation move`.
5. **Prefer links for large data**: `--operation link` avoids copying large source trees.
6. **Keep the audit trail**: archive `project.log` alongside the dataset; it records exactly what each run did.

For the full command and option reference, see the [Overview and CLI Usage Guide](overview.md). To integrate Marimba
into Python applications rather than shell scripts, see the [API Reference](api.md).
