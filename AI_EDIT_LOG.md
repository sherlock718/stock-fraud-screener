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

---

## Next Claude Session Handoff

- Status: Session 1 complete
- Branch: `refactor/s1-atlas` (local commit, not pushed, not merged)
- Files created: `PIPELINE_ATLAS.md`, `PARQUET_ATLAS.md`, updated `AI_EDIT_LOG.md`
- Guardrails: pre-commit hook active from Session 0
- Key risk identified: `historical_dataset_clean.parquet` mutation chain has no enforced ordering or idempotency check
- Next goal: Session 2 — Write foundational tests for pipeline steps 2, 3, and 5 (the P0 priority items from Test Matrix). Use synthetic data only. No network calls. Validate temporal integrity (no look-ahead bias) and schema contracts.
- Branch flow: User merges `refactor/s1-atlas` into `main`, then Session 2 creates `refactor/s2-tests` from updated `main`
