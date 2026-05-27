# Regression-harness golden snapshot

This directory holds the checked-in golden expected output that
`tests/e2e/regression/` compares fresh runs against.

## Files

| File | What it is |
|---|---|
| `invariants.json` | Structural totals: file counts by class, per-pipeline / per-collection / per-kind counts, top-level-file inventory, scrubbed-manifest sha256, manifest line count. Small, human-reviewable. |
| `manifest.scrubbed.txt` | The dataset's `manifest.txt` with every hash replaced by a hash of the file's *scrubbed* bytes (per `scrub.py`). Entries with intrinsically volatile content (log files, plus `map.png` whose tiles come from a remote OSM-backed server and drift over wall-clock time) keep their path slot but their hash is replaced by `SHA256_PLACEHOLDER`. |
| `regenerate.py` | Generator script. Re-run after any deliberate marimba or pipeline change rotates the expected output. |

## Regenerating

```bash
# 1. Drive a fresh full bootstrap to produce a packaged dataset. Phase 4+ will
#    expose a fixture for this; for now use the Phase 2 characterisation script
#    (also fine — it writes two identical projects under temp/phase2-determinism/).
uv run python temp/phase2-determinism/characterise.py

# 2. Regenerate the goldens from either project's dataset.
uv run python tests/e2e/regression/golden/regenerate.py \
    temp/phase2-determinism/project-a/datasets/IN2018_V06

# 3. Inspect the diff. Anything you didn't intend to change is a regression
#    that needs investigating, not a golden-rotation.
git diff tests/e2e/regression/golden/

# 4. If the diff matches the deliberate change you made, commit the rotation
#    in the same commit as the marimba change that caused it, with a body
#    explaining what shifted and why.
```

## When to rotate

A golden rotation should be **explicit and reviewed**. Cases:

- A marimba refactor changes the dataset's directory layout, file naming, or per-file content shape.
- A pipeline-SHA bump in `tests/e2e/regression/conftest.py` (`PIPELINE_REPO_SHA`) changes what the MRITC pipeline emits.
- A data-SHA bump (`DATA_REPO_SHA`) replaces the upstream fixture.
- An intentional change to the scrubber itself (e.g. adding a new volatile field to scrub).

If a Tier C test fails in CI on a branch that didn't intend any of the above, that's a regression — fix the production code, don't rotate the golden.

## Scrubber correctness

The scrubber is proven correct iff regenerating from two independent
identical-input bootstraps produces byte-identical `invariants.json` and
`manifest.scrubbed.txt`. The Phase 2 characterisation establishes this; if
the scrubber is ever extended, re-run that proof:

```bash
uv run python temp/phase2-determinism/characterise.py
uv run python tests/e2e/regression/golden/regenerate.py \
    temp/phase2-determinism/project-a/datasets/IN2018_V06
cp tests/e2e/regression/golden/{invariants.json,manifest.scrubbed.txt} /tmp/a/
uv run python tests/e2e/regression/golden/regenerate.py \
    temp/phase2-determinism/project-b/datasets/IN2018_V06
diff /tmp/a/invariants.json tests/e2e/regression/golden/invariants.json
diff /tmp/a/manifest.scrubbed.txt tests/e2e/regression/golden/manifest.scrubbed.txt
# both diffs must be empty
```
