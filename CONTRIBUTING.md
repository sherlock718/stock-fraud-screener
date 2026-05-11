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
2. Add tests in `tests/test_pipeline.py` (extend `_make_annual_df()` if new columns needed)
3. Update `docs/` if the feature changes user-visible behavior
4. Add a CHANGELOG.md entry under `[Unreleased]`
5. PR to `develop`; after review, promote develop → staging → main

## Required Secrets (for contributors with deploy access)

| Secret | Purpose |
|--------|---------|
| `HF_TOKEN` | HuggingFace write access |
| `HF_REPO` | Dataset repo ID |

Set locally in `.env` (never commit `.env`). In GitHub Actions, set under Settings → Secrets.
