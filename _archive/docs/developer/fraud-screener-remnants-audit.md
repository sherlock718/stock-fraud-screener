# Fraud-Screener Remnants Audit

> Session 6 artifact (2026-06-24). Documents all "fraud screener" project-identity
> remnants and classifies each as keep / safe rename / defer / archive.

---

## Context

The project identity is: **Multi-Factor Stock Screener & Alpha Generation Platform**.
Fraud risk is one of five factor groups (Value · Quality · Momentum · Growth · Fraud Risk).
The old "stock fraud screener" name persists in the GitHub repo URL and some docs.

---

## Findings

### 1. GitHub Repo Name: `stock-fraud-screener`

| Item | Value |
|---|---|
| Location | GitHub URL, clone commands, HF_REPO env var |
| Files affected | `docs/quickstart.md:13-14`, `docs/developer/setup.md:13-14,31,87,96`, `docs/developer/deployment.md:60,82` |
| Verdict | **Defer** |
| Reason | Renaming the repo breaks all external links, clone instructions, CI secrets, HuggingFace integration. Must be coordinated as a single atomic rename across GitHub + HuggingFace + CI. Not a docs-only change. |
| Proposed action | Session 7+ dedicated rename session when ready to break links |

### 2. CONTEXT.md line 10 — "NOT a fraud screener"

| Item | Value |
|---|---|
| Location | `CONTEXT.md:10` |
| Current text | `**Renaissance-style quantitative alpha lab.** NOT a fraud screener.` |
| Verdict | **Keep** |
| Reason | This is a corrective statement explicitly clarifying the project is NOT a fraud screener. It serves the right purpose. |

### 3. CLAUDE.md line 7 — "not a fraud screener"

| Item | Value |
|---|---|
| Location | `CLAUDE.md:7` |
| Current text | `not a fraud screener. Fraud risk is one of five factors` |
| Verdict | **Keep** |
| Reason | Same as above — corrective framing for AI assistants. Accurate and useful. |

### 4. docs/guide/app.md — Streamlit tab documentation

| Item | Value |
|---|---|
| Location | `docs/guide/app.md` (entire file) |
| Content | Documents 10 Streamlit tabs including "Screener", "Case Studies", fraud score UI |
| Verdict | **Keep (archived UI reference)** |
| Reason | File already has a deprecation notice at line 3 ("app_v2.py is archived"). The "Screener" tab name and fraud case studies are legitimate product features that exist in the alpha/factors system. "Screener" is a valid financial term (stock screener). The fraud case studies document real methodology. |

### 5. docs/methodology/case-studies.md — Fraud case library

| Item | Value |
|---|---|
| Location | `docs/methodology/case-studies.md` |
| Content | 15 documented accounting fraud cases with quantitative signals |
| Verdict | **Keep** |
| Reason | This is legitimate fraud-risk factor methodology. The cases validate that fraud signals (Beneish, Altman, etc.) have predictive power. This is core research, not project-identity framing. |

### 6. docs/methodology/benchmarking.md — Signal AUC benchmarks

| Item | Value |
|---|---|
| Location | `docs/methodology/benchmarking.md` |
| Content | AUC-ROC benchmarks of fraud-detection signals vs `fraud_confirmed` labels |
| Verdict | **Keep** |
| Reason | Legitimate methodology — measuring how well each fraud signal discriminates. "Fraud detection" here is describing signal evaluation, not the project purpose. This is factor research. |

### 7. docs/guide/strategies.md — Strategy definitions

| Item | Value |
|---|---|
| Location | `docs/guide/strategies.md` |
| Content | References "fraud-safe", "low fraud score", "fraud avoidance" |
| Verdict | **Keep** |
| Reason | These describe legitimate investment strategies that use fraud risk as a filter. "Fraud-safe long" means "long positions filtered for low fraud risk". This is correct alpha terminology. |

### 8. docs/developer/pipeline-scripts.md — Fraud Signal Modules section

| Item | Value |
|---|---|
| Location | `docs/developer/pipeline-scripts.md:175-216` |
| Content | Documents fraud_signals.py (archived) and fraud taxonomy columns |
| Verdict | **Keep** |
| Reason | Documents real pipeline columns (`fraud_score_*`) that exist in the dataset. Already marked as archived. The fraud scores are legitimate features used in the 5-factor alpha system. |

### 9. docs/quickstart.md line 3 — "Get the screener running"

| Item | Value |
|---|---|
| Location | `docs/quickstart.md:3` |
| Current text | `Get the screener running in 5 minutes.` |
| Verdict | **Safe rename** |
| Reason | "Screener" alone is fine (stock screener), but could be clearer. |
| Fix | Change to "Get the platform running in 5 minutes." |

### 10. docs/developer/contributing.md line 173 — "fraud-screener framing"

| Item | Value |
|---|---|
| Location | `docs/developer/contributing.md:173` |
| Current text | `the same problem that caused the fraud-screener framing to persist for multiple sessions` |
| Verdict | **Keep** |
| Reason | This is a meta-comment about the naming problem itself — it's correct and serves as documentation of why sync rules exist. |

### 11. docs/developer/data-update-guide.md — "Refresh fraud taxonomy scores"

| Item | Value |
|---|---|
| Location | `docs/developer/data-update-guide.md:228,250-253` |
| Content | Fraud score validation checks in data refresh workflow |
| Verdict | **Keep** |
| Reason | Legitimate operational documentation for fraud-risk factor columns. |

### 12. ROADMAP.md — fraud_score_governance, fraud_suspect bugs

| Item | Value |
|---|---|
| Location | `ROADMAP.md:60-61,65` |
| Content | Bug entries for `fraud_score_governance` NaN and `fraud_suspect` zero |
| Verdict | **Keep** |
| Reason | Active bug tracking for real columns. These are data quality issues in the fraud-risk factor, not project-identity problems. |

---

## Summary

| Category | Count | Action |
|---|---|---|
| Keep (corrective/methodology/factor) | 10 | No change |
| Safe doc rename | 1 | Fix in this session |
| Defer (repo rename) | 1 | Future session |
| Archive candidate | 0 | — |

---

## Conclusion

The project identity is already correct in all primary documents (README, docs/index.md,
CONTEXT.md, CLAUDE.md). The remaining "fraud" references are either:

1. **Corrective** — explicitly saying "NOT a fraud screener"
2. **Factor methodology** — documenting fraud-risk as one of five factors
3. **Repo URL** — can only be fixed by renaming the GitHub repo (deferred)

Only one trivial wording fix needed (quickstart.md tagline).
