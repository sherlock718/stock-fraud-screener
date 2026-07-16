# Codex Workflow — Low Token Burn

This file defines the default operating pattern for Codex in this repository.
The user should normally need to describe only the desired outcome.

## Task Boundaries

Use one task per coherent outcome, normally one of:

1. Read-only audit or diagnosis.
2. Focused implementation with scoped tests.
3. Dataset/model execution after the implementation is accepted.
4. Controlled comparison and interpretation.
5. Documentation and handoff.

Do not combine repository orientation, implementation, multi-hour data rebuilds,
model training, backtesting, documentation, and commits into one task.

## Default Cost Controls

- Let Codex choose the model and reasoning automatically unless the user specifies them.
- Classify PIT correctness, target availability, fitted transformations,
  temporal splits, backtest validity, and financial claims as critical work.
- Classify documentation, manifests, formatting, deterministic scripts, and
  already-specified mechanical changes as routine work.
- Do not use Ultra or subagents by default. They increase total model work.
- Keep Fast mode disabled.
- Do not connect or call MCP servers, apps, web search, or external APIs unless
  required for the current outcome.
- Do not inspect telemetry or estimate token/cost usage unless the user asks.

## Context Controls

- Read `docs/START_HERE.md` and `docs/CODEX_HANDOFF.md` first.
- Read targeted source files; avoid broad recursive reads without evidence that
  they are necessary.
- Prefer bounded searches such as `rg` over loading entire directories.
- Use quiet verification (`pytest -q`, targeted test paths, bounded output).
- Keep raw logs out of chat. Store them under a temporary directory if needed.
- Do not repeat an investigation already captured in the handoff.
- Update the handoff before switching modules or starting a new task.
- Batch the initial bounded searches/reads, and do not reread unchanged files
  later in the same task.
- Use `git diff --stat`, `git diff --check`, and path-scoped excerpts instead of
  printing the cumulative dirty-worktree diff.

## Risk-Based Execution

### Critical methodology work

Examples: PIT data, label end dates, survivorship, preprocessing fit boundaries,
walk-forward folds, OOS predictions, monthly NAV, drawdown, and performance claims.

Required pattern:

1. Inspect actual code and data without editing.
2. Separate confirmed evidence, inference, and unknowns.
3. Propose the smallest correction and its tests.
4. Implement only after the correction is well-defined.
5. Re-run controlled comparisons with frozen inputs.

### Routine work

Examples: docs, manifests, path corrections, test naming, deterministic hashing,
and small refactors with established behavior.

Use narrow reads, minimal edits, and scoped tests. Do not escalate reasoning or
expand scope unless evidence requires it.

## Data and Artifact Safety

- Never overwrite the only copy of a dataset, model, prediction file, or report.
- Freeze comparisons under a structured artifact directory, not scattered
  `_OLD` files.
- Do not refresh external prices during OLD-versus-CORRECTED comparisons.
- Record hashes, code commit, dirty state, configuration, dependency versions,
  and random seeds for model/backtest runs.
- Do not commit or push large generated artifacts unless explicitly requested.

## Verification

- During edits: run only tests related to the changed module.
- At the final boundary: run the full suite once when the change can affect
  shared behavior or production output. Documentation-only closeout after that
  requires only targeted document checks; do not repeat the full suite.
- Summarize commands, pass/fail counts, warnings, and unverified boundaries.
- Never treat a performance threshold as proof of methodological correctness.

## Session Close

Update `docs/CODEX_HANDOFF.md`. Keep it concise and factual so the next task can
resume without rereading long conversations or historical plans.
