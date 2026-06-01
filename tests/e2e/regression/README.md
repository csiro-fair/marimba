# mritc-demo end-to-end regression harness

The load-bearing safety net against codebase-wide refactors silently breaking real dataset processing. Drives the published [`mritc-demo-pipeline`](https://github.com/csiro-fair/mritc-demo-pipeline) over the published [`mritc-demo-data`](https://github.com/csiro-fair/mritc-demo-data) fixture through the full marimba lifecycle (`new project` → `new pipeline` → `import` × 10 → `process` → `package`) and asserts the resulting FAIR-packaged dataset matches a checked-in golden snapshot across three independent tiers.

## When this matters

Run it (or let the pre-push hook run it) after:

- Any change in `marimba/` you suspect could affect dataset output.
- Closing any review-cycle plan from `docs/prompts/` — it's mandated by the canonical Stage 2 closure (see [`docs/prompts/README.md`](../../../docs/prompts/README.md) §"Verification before closure").
- Bumping a marimba dependency that touches an output path (Pillow / pyexiftool / staticmap / iFDO).
- Anything else that gives you a "I think this is safe but…" feeling.

## Layout

```
tests/e2e/regression/
├── conftest.py                       # cache fixtures, MarimbaRunner, packaged_dataset
├── scrub.py                          # YAML / JPEG / manifest scrubbers (Phase 2 characterisation)
├── test_smoke.py                     # smoke: new project + new pipeline + 10x import
├── test_regression.py                # tiers A / B / C against golden/
└── golden/
    ├── invariants.json               # structural totals (small, reviewable)
    ├── manifest.scrubbed.txt         # ~1680-line scrubbed manifest
    ├── regenerate.py                 # golden-rotation entry point
    └── README.md                     # rotation workflow + scrubber-correctness proof
```

## How it runs

| Test | What it asserts | Cold-cache | Warm-cache |
|---|---|---:|---:|
| `test_smoke_cache_and_import` | Cache populates, project bootstrap + 10x import succeed. | ~30s | ~6s |
| `test_tier_a_structural` | Dataset layout, file presence, YAML parsability, per-collection ifdo YAMLs for all 10 collections. | shared* | shared* |
| `test_tier_b_inventory` | Structural counts match `golden/invariants.json` (by-class, top-level files, per-pipeline/collection/kind totals, manifest line count). | shared* | shared* |
| `test_tier_c_scrubbed_manifest_byte_equality` | Fresh-run scrubbed manifest byte-matches `golden/manifest.scrubbed.txt`. Load-bearing. | shared* | shared* |

\* The three tiers share one `packaged_dataset` bootstrap per `pytest` invocation (`scope="module"`). Cold ~60s, warm ~30s for all three combined.

Full suite from a clean checkout: **~37s warm cache, ~90s cold cache**.

## Running locally

```bash
# All four tests (smoke + three tiers)
uv run pytest --rootdir . -c config/pytest.ini --no-cov tests/e2e/regression/ -v

# Just the three regression tiers (one shared bootstrap)
uv run pytest --rootdir . -c config/pytest.ini --no-cov tests/e2e/regression/test_regression.py -v

# A single tier (fastest iteration on a specific failure)
uv run pytest --rootdir . -c config/pytest.ini --no-cov tests/e2e/regression/test_regression.py -k tier_c -v

# Force a fresh data + pipeline clone (validates the SHA-pin path)
rm -rf .cache/marimba-e2e && uv run pytest --rootdir . -c config/pytest.ini --no-cov tests/e2e/regression/ -v

# Per-stage timings inside the bootstrap
uv run pytest --rootdir . -c config/pytest.ini --no-cov tests/e2e/regression/ -v --durations=0
```

## Pre-push hook setup (one-time per checkout)

```bash
uv run pre-commit install --hook-type pre-push
```

Without this the hook is configured but won't fire. Once installed, every `git push` runs the regression suite first; failures block the push. Bypass for genuinely doc-only pushes:

```bash
git push --no-verify   # use sparingly
```

## Cache

`<repo>/.cache/marimba-e2e/` holds the cloned demo repos at the pinned SHAs declared in `conftest.py` (`DATA_REPO_SHA`, `PIPELINE_REPO_SHA`). The cache is git-ignored (`.gitignore:46 .cache`) so it stays out of commits and `git status`. To share across worktrees or point at a CI cache volume:

```bash
MARIMBA_E2E_CACHE_DIR=/path/to/shared/cache uv run pytest ...
```

To force a refresh of either repo, delete the relevant subdir (e.g. `rm -rf .cache/marimba-e2e/data`) — the conftest will re-clone on next invocation.

## Bumping the pinned SHAs

When you want to track upstream changes in either repo:

1. Find the new SHA on GitHub (e.g. `https://github.com/csiro-fair/mritc-demo-data/commits/main`).
2. Update `DATA_REPO_SHA` or `PIPELINE_REPO_SHA` in `conftest.py`.
3. Regenerate the goldens (see [`golden/README.md`](golden/README.md)).
4. Commit all three (conftest SHA bump + both regenerated goldens) together so the rotation is bisectable.

## Debugging failures

The tiers are ordered cheapest → most precise. If you see a failure, start at the tier that fired and work down:

### Tier A — `test_tier_a_structural`

Asserts the dataset's shape exists. Failures point at a missing file, malformed YAML, or empty top-level artifact.

**Common causes:**

- A required marimba CLI command silently no-op'd (e.g. `package` exited zero but didn't write `summary.md`).
- YAML serialiser changed format in a way pyyaml can't parse.
- The dataset name (`PACKAGED_DATASET_NAME` in `conftest.py`) doesn't match what `marimba package` actually emitted.

**To inspect:** the failed run's project dir lives under `/tmp/pytest-of-<user>/pytest-current/scratch0/project/`. Pytest cleans it at session end; run a single tier and inspect before another pytest invocation:

```bash
uv run pytest --rootdir . -c config/pytest.ini --no-cov tests/e2e/regression/test_regression.py -k tier_a --basetemp=/tmp/marimba-e2e-debug -v
ls /tmp/marimba-e2e-debug/scratch0/project/datasets/IN2018_V06/
```

### Tier B — `test_tier_b_inventory`

Asserts structural counts. Failures dump the exact category that drifted (e.g. "inventory drift at 'by_class' expected={…} actual={…}").

**Common causes:**

- Pipeline output count changed (new file class introduced, or a file class disappeared).
- Collection-level structure changed (e.g. `thumbnails/` directory renamed).
- Marimba changed manifest line count (added or removed an entry).

**To resolve:**

1. Read the failure message — it names the specific dict that doesn't match.
2. Decide: is this drift you intended? If yes, regenerate the goldens (see below). If no, this is the regression you set out to catch — fix the code.

### Tier C — `test_tier_c_scrubbed_manifest_byte_equality`

The load-bearing tier. Asserts every dataset file's scrubbed content matches the golden, with three classes of carve-out (entries kept in the manifest for path + ordering, hash replaced with placeholder): log files under `logs/`, `map.png`, and all JPEG images. `map.png` is excluded because `staticmap` renders by fetching tiles from a remote OSM-backed server, so its bytes change over wall-clock time as upstream tile data updates — same project state produces different bytes a day later. Failures dump the first 20 differing manifest lines plus the line-count delta if any.

JPEG content is excluded because its encoded bytes are not reproducible across CPU microarchitectures. On GitHub's mixed hosted-runner fleet a handful of borderline frames flipped between two values depending on which runner CPU executed the job: numpy's SIMD reductions and libjpeg's encoder round differently between CPU generations, and that drift shows up in the main entropy-coded scan, the embedded EXIF thumbnail's own scan, and the `image-entropy` / `image-average-color` values baked into `EXIF:UserComment`. There is no stable byte subset to hash, so JPEG content is not byte-compared at all. The image's metadata is still byte-validated through its per-image iFDO YAML (scrubbed for the same volatile fields), and Tier A/B assert each image's presence, validity, and counts.

**Common causes:**

- A new volatile field appeared in the YAML output that the scrubber doesn't know about (the scrubber currently handles `image-uuid`, `image-set-uuid`, `image-hash-sha256`, `image-entropy`, `image-average-color`). Add the field to `scrub.py::_YAML_FIELD_PATTERNS`.
- The pipeline source-copy under `pipelines/MRITC/repo/` drifted (pipeline-repo SHA bumped without rotating the golden).
- An output file the manifest covers is genuinely different.

**To resolve:**

1. Look at which manifest lines differ. The path tells you which file changed.
2. Compare the failing file against a fresh independent run to confirm it's a deterministic drift (regression) and not a scrubber gap. The `temp/phase2-determinism/characterise.py` script is the one-shot way to do this — it produces two independent runs and a per-file diff report.
3. If it's a scrubber gap, extend `scrub.py` and re-verify against both Phase 2 runs (see [`golden/README.md`](golden/README.md) §"Scrubber correctness").
4. If it's intended drift, regenerate the goldens.
5. If it's an actual regression, fix the underlying code.

## Rotating the goldens

When a deliberate marimba or pipeline change shifts the expected output, the goldens need to be regenerated. See [`golden/README.md`](golden/README.md) for the full workflow — short version:

```bash
# Produce a fresh packaged dataset (the Phase 2 characterisation script is one way)
uv run python temp/phase2-determinism/characterise.py

# Regenerate the goldens
uv run python tests/e2e/regression/golden/regenerate.py \
    temp/phase2-determinism/project-a/datasets/IN2018_V06

# Inspect the diff
git diff tests/e2e/regression/golden/

# Commit the rotation in the same commit as the code change that caused it
```

## CI gate

`.github/workflows/ci-e2e.yml` runs the suite on every push to main + every PR + manual `workflow_dispatch`. Uses `actions/cache` keyed on the pinned-SHA hash of `conftest.py` so subsequent runs skip the data clone.

The workflow itself doesn't gate merges — that's a GitHub repo setting under Branches → Branch protection rules → Require status checks → select `E2E regression (Ubuntu, Python 3.12)`.

## Design background

The harness was designed in six phases; per-phase rationale and findings live in commits `d8c91fc` (cache + smoke), `43d2197` (scrubber + goldens), `1b95239` (tiered tests), `1f98966` (CI + hook). The Phase 2 determinism characterisation that drove the scrubber design is captured in `temp/phase2-determinism/REPORT.md` (gitignored throwaway; re-run anytime via `temp/phase2-determinism/characterise.py`).

The headline finding from Phase 2: marimba generates a fresh `uuid.uuid4()` per image at `process` time and embeds it in EXIF (both `ImageUniqueID` and inside the `UserComment` JSON blob). That UUID cascades into the per-image YAML, the dataset-level rollup, and the manifest. The scrubber normalises this cascade so byte-equality of the *scrubbed* manifest is a meaningful regression assertion. A content-addressed UUID scheme (e.g. UUIDv5 over the source-image hash) would make scrubbing unnecessary; that's a candidate finding for a real `codebase-review` cycle, not a blocker here.
