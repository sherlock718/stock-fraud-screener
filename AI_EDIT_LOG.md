# AI Edit Log

Record of all AI-assisted changes, session by session.

---

## Session 0 — Guardrails Setup (2026-06-20)

**Branch:** `refactor/s0-guardrails`

**Files created:**
- `.claude/settings.json` — read-only bash permissions (grep, find, ls, wc, git log/status/diff)
- `scripts/hooks/pre_commit_guard.py` — tracked pre-commit guardrail script
- `.git/hooks/pre-commit` — calls tracked script (not committed, local only)
- `KNOWN_ISSUES.md` — issue tracker with Parking Lot section
- `AI_EDIT_LOG.md` — this file

**Guardrails active:**
- Pre-commit warns if >5 files staged
- Pre-commit warns if pipeline/step* changes without tests/pipeline/ change
- Pre-commit blocks data/*.parquet commits
- Branch-per-session enforced
- No production code changes in Sessions 0–1

**Known issues logged:**
- LIQUIDITY-001: No liquidity/volume feature in main dataset (high priority)

---

## Session 1 — Pipeline & Parquet Atlas (2026-06-21)

**Branch:** `refactor/s1-atlas`

**Files created:**
- `PIPELINE_ATLAS.md` — Full pipeline reference map (File Map, Call Graph, Test Matrix, Step Audit Checklist, Feature Coverage Matrix)
- `PARQUET_ATLAS.md` — Every parquet file: creator, readers, mutators, keys, type

**Key findings:**
- 33 files in `pipeline/`. 18 are CURRENT_CORE (step1→step6 chain), 5 CURRENT_SUPPORT, 1 QA utility, 9 LEGACY_ARCHIVE_CANDIDATE (JSON-era, operate on `companies_financials.json`)
- 3 legacy modules import `pipeline.score_and_report` which DOES NOT EXIST (broken imports, dead code path)
- `historical_dataset_clean.parquet` is mutated in-place by 16+ scripts post-step6 (fragile, order-dependent)
- Test coverage: only `tests/test_pipeline.py` exists — covers train_models logic only. Zero coverage for pipeline steps 1–6
- Liquidity features (ADTV, bid-ask) exist only in legacy JSON pipeline. Not connected to parquet pipeline
- Insider signals, governance, market signals — same: legacy JSON only, not in step5

**No production code changes. No test files created. Code-reading and grep only.**

**Session-end checklist:**
- Atlas update needed? No. Created this session.
- Parquet atlas update needed? No. Created this session.
- KNOWN_ISSUES update needed? Yes. Added BROKEN-IMPORT-001, MUTATION-ORDER-001, TEST-COVERAGE-001.
- AI_EDIT_LOG handoff updated? Yes.

---

## Living-Doc Maintenance Rules

`PIPELINE_ATLAS.md` and `PARQUET_ATLAS.md` are living documents. Every future session must update them if it changes pipeline file roles, file classifications, imports/call graph, parquet creators/readers/mutators, archive status, test coverage status, or feature ownership/layer classification. If no atlas update is needed, the session report must say why.

Before ending any session, check whether `PIPELINE_ATLAS.md`, `PARQUET_ATLAS.md`, `KNOWN_ISSUES.md`, or `AI_EDIT_LOG.md` needs updating. If not, say why.

---

## Next Claude Session Handoff

- Status: Session 1 complete (with handoff corrections applied)
- Branch: `refactor/s1-atlas` (local commit, not pushed, not merged)
- Files changed: `PIPELINE_ATLAS.md`, `PARQUET_ATLAS.md`, `AI_EDIT_LOG.md`, `KNOWN_ISSUES.md`, `scripts/hooks/pre_commit_guard.py`
- Guardrails: pre-commit hook active (parquet block + pipeline-without-tests warning + docs-consistency warning)
- Next goal: **Session 2 — Audit Steps 1–3, then add minimal critical tests for Step 1, Step 2, and Step 3 only.**
- Branch flow: User merges `refactor/s1-atlas` into `main`, then Session 2 creates `refactor/s2-audit-steps-1-3` from updated `main`

### Session-end checklist (template for all future sessions)

- Atlas update needed? Yes/No. Reason:
- Parquet atlas update needed? Yes/No. Reason:
- KNOWN_ISSUES update needed? Yes/No. Reason:
- AI_EDIT_LOG handoff updated? Yes/No.

### Session 2 prompt (copy-paste into a fresh Claude Code conversation):

```
Start Session 2. Project: /Users/mhoque/Desktop/stock-fraud-screener-main

First, verify setup:
1. Run git status — confirm clean working tree
2. Confirm current branch is main
3. Create and checkout branch: git checkout -b refactor/s2-audit-steps-1-3

Then read these files to understand the codebase state:
- PIPELINE_ATLAS.md (file map, call graph, test matrix, step audit checklist)
- PARQUET_ATLAS.md (parquet file registry, mutation order)
- KNOWN_ISSUES.md (logged issues from Sessions 0–1)
- AI_EDIT_LOG.md (session history and handoff)

Session 2 goal: Audit Steps 1–3, then add minimal critical tests for Step 1, Step 2, and Step 3 only.

Scope:
- Audit Step 1 (ticker universe): schema, dedup, survivorship, identifiers
- Audit Step 2 (snapshots): one-row-per-key, filed_date integrity, currencies, units
- Audit Step 3 (prices): entry price after filed_date, forward returns future-only, momentum past-only, survivorship handling
- Inspect existing tests/ folder first to understand current coverage
- Write minimal critical tests for Steps 1–3 only (synthetic data, no network calls)
- Tests must validate temporal integrity (no look-ahead bias) and schema contracts

Rules:
- No broad refactor
- No feature engineering
- No archive/move/delete of pipeline files
- If a critical issue is found during audit, classify it, propose the smallest fix, and wait for approval before changing production code
- Step 5 is NOT in scope — that belongs to Session 4

At session end, update:
- KNOWN_ISSUES.md — add any new issues found during audit
- PIPELINE_ATLAS.md — update Test Matrix status column if tests are added
- AI_EDIT_LOG.md — add Session 2 report + Session 3 handoff with full prompt

Before ending the session, check whether PIPELINE_ATLAS.md, PARQUET_ATLAS.md, KNOWN_ISSUES.md, or AI_EDIT_LOG.md needs updating. If not, say why.

Session-end checklist:
- Atlas update needed? Yes/No. Reason:
- Parquet atlas update needed? Yes/No. Reason:
- KNOWN_ISSUES update needed? Yes/No. Reason:
- AI_EDIT_LOG handoff updated? Yes/No.

Commit locally at end. Do not push. Do not merge.
```
