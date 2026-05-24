"""End-to-end regression harness for the mritc-demo pipeline.

Drives the published `mritc-demo-pipeline` over the published `mritc-demo-data`
fixture and asserts that the resulting packaged dataset matches a golden
snapshot. The intent is to catch regressions introduced by codebase-wide
refactors (e.g. those produced by `docs/prompts/*-review.md` cycles).
"""
