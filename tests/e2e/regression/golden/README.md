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
# 1. Run the regression suite to drive a fresh full bootstrap. The Tier C test
#    will fail against the stale goldens — that's expected; the run still leaves
#    a complete packaged dataset in pytest's tmp tree.
uv run pytest --rootdir . -c config/pytest.ini --no-cov -m "e2e and slow" tests/e2e/regression/

# 2. Regenerate the goldens from that dataset (pytest keeps the last few runs;
#    pick the newest scratch dir).
uv run python tests/e2e/regression/golden/regenerate.py \
    "$(ls -dt /tmp/pytest-of-$USER/pytest-*/scratch*/project/datasets/IN2018_V06 | head -1)"

# 3. Inspect the diff. Anything you didn't intend to change is a regression
#    that needs investigating, not a golden-rotation.
git diff tests/e2e/regression/golden/

# 4. Re-run the suite — it must now pass against the rotated goldens.
uv run pytest --rootdir . -c config/pytest.ini --no-cov -m "e2e and slow" tests/e2e/regression/

# 5. If the diff matches the deliberate change you made, commit the rotation
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
`manifest.scrubbed.txt`. If the scrubber is ever extended, re-run that proof
by bootstrapping twice (each regression-suite run produces a fresh dataset
under a new pytest scratch dir) and regenerating from each:

```bash
mkdir -p /tmp/a
uv run pytest --rootdir . -c config/pytest.ini --no-cov -m "e2e and slow" tests/e2e/regression/
uv run python tests/e2e/regression/golden/regenerate.py \
    "$(ls -dt /tmp/pytest-of-$USER/pytest-*/scratch*/project/datasets/IN2018_V06 | head -1)"
cp tests/e2e/regression/golden/{invariants.json,manifest.scrubbed.txt} /tmp/a/
uv run pytest --rootdir . -c config/pytest.ini --no-cov -m "e2e and slow" tests/e2e/regression/
uv run python tests/e2e/regression/golden/regenerate.py \
    "$(ls -dt /tmp/pytest-of-$USER/pytest-*/scratch*/project/datasets/IN2018_V06 | head -1)"
diff /tmp/a/invariants.json tests/e2e/regression/golden/invariants.json
diff /tmp/a/manifest.scrubbed.txt tests/e2e/regression/golden/manifest.scrubbed.txt
# both diffs must be empty
```
