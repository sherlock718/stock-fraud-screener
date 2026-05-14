# Contributing

## Branch Strategy

```
main        ← production; tagged releases only
develop     ← integration branch; all feature branches merge here
feature/*   ← individual features (branch from develop)
fix/*       ← bug fixes (branch from develop)
staging     ← pre-production testing (promote from develop before main)
```

Never push directly to `main`. Open a PR from `develop` → `main` for releases.

## Development Workflow

```bash
git checkout develop
git pull origin develop
git checkout -b feature/my-feature

# ... make changes ...

git add <files>
git commit -m "feat: short description"
git push origin feature/my-feature

# Open PR: feature/my-feature → develop
```

## Commit Message Convention

```
feat:    new feature
fix:     bug fix
perf:    performance improvement
refactor: code restructuring (no behavior change)
test:    adding or updating tests
docs:    documentation only
chore:   build/tooling/CI changes
```

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

All tests use synthetic in-memory data — no files, no network, no model files required. See `docs/developer/tests.md` for full test documentation.

## Code Standards

- Python 3.11+
- Line length: 100 characters (not enforced by linter, but preferred)
- Type hints on all public functions
- No dead code — delete unused functions rather than commenting them out
- No TODO comments in committed code — open a GitHub issue instead

## Adding New Features

1. Branch from `develop`
2. Implement the change
3. Work through the **Sync Checklist** below
4. Add tests in `tests/test_pipeline.py` (extend `_make_annual_df()` if new columns needed)
5. PR to `develop`; after review, promote develop → staging → main

---

## Sync Checklist

Every change must pass this checklist before the PR is opened. All items that apply
must be completed in the **same commit** as the code change — not a follow-up.

| Changed area | Required doc/diagram update |
|---|---|
| New script in `scripts/` | `docs/developer/scripts.md` — add section with usage, flags table, output files |
| Modified script CLI flags | `docs/developer/scripts.md` — update flags table for that script |
| New pipeline step or data column | `docs/architecture.md` — Component Map + Data Flow diagram + column count in flowchart labels |
| Dataset column count changes | All Mermaid nodes that reference the count: `docs/architecture.md` + `docs/methodology/models.md` |
| Model performance changes | `docs/methodology/models.md` AUC table — Val AUC, Test AUC, WF Mean AUC, target flag |
| ML pipeline structural change | Mermaid flowchart in `docs/methodology/models.md` + ML System subgraph in `docs/architecture.md` |
| New system component (DB, API, service) | `docs/architecture.md` — new node in High-Level Overview + row in Component Map |
| New API endpoint | Docstring in route file |
| New UI tab or major UI feature | `docs/guide/app.md` |
| **Any of the above** | `CHANGELOG.md` — entry under `[Unreleased]`, bold the script/file name |

If you are unsure which rows apply, see `CLAUDE.md` for the full Architecture Sync Rules
and the Current Architecture State table.

## Required Secrets (for contributors with deploy access)

| Secret | Purpose |
|--------|---------|
| `HF_TOKEN` | HuggingFace write access |
| `HF_REPO` | Dataset repo ID |

Set locally in `.env` (never commit `.env`). In GitHub Actions, set under Settings → Secrets.
