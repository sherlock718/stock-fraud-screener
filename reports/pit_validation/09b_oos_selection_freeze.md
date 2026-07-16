# Session 9B — OOS Selection Freeze

Date: 2026-07-16

Status: **Complete — frozen unavailable verdict**

## Outcome

No compatible, explicitly named `CORRECTED_8F` selection path is complete, so
Session 9B freezes zero holdings and Session 9C remains blocked. The Session 9
calibrated logistic classifier was not substituted for the production
tree-agreement role, and no threshold, fallback, smaller portfolio, or weight
renormalization was introduced.

## Session 9 preflight

The exact Session 9 manifest `bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb` and all
7 validated inputs, 846
artifact records, 4 code-lineage records, and
3 hash-covered dirty-state references passed
before any Session 9B artifact was written.

## Reconciled blockers

- `production_ml_gates`: missing compatible OOS tree_agreement_gate:3y; Session 9 classification is calibrated logistic; required composite gate fields are not frozen in the Session 9 prediction table; canonical ADTV eligibility is not frozen; accepted entry timestamp is absent from the Session 9 prediction lineage
- `engine_composite`: value_composite and quality_composite are absent from CORRECTED_8F; canonical ADTV eligibility is not frozen; no accepted Session 9B market/weighting variant resolves current configuration ambiguity; accepted entry timestamp is absent from the Session 9 prediction lineage
- `engine_qem`: eps_growth_yoy, value_composite, and quality_composite are absent from CORRECTED_8F; canonical ADTV eligibility is not frozen; accepted entry timestamp is absent from the Session 9 prediction lineage
- `engine_scdv`: value_composite, quality_composite, PB, Beneish score, and Altman score are absent from CORRECTED_8F; canonical ADTV eligibility is not frozen; accepted entry timestamp is absent from the Session 9 prediction lineage
- `engine_iarb`: CORRECTED_8F contains only the certified US SEC population; value_composite, quality_composite, PB, and Beneish score are absent from CORRECTED_8F; canonical ADTV eligibility is not frozen; accepted entry timestamp is absent from the Session 9 prediction lineage

The production path retains its accepted top-15, descending 3y regression,
equal-weight, hard-gate, and 1%-ADTV configuration as documentation only. It is
not executable because the required OOS tree-agreement probability is absent.
The Session 9 logistic threshold was neither inferred nor optimized.

## Frozen row-level evidence

Both physical population namespaces retain separate candidate/exclusion and
holding tables. The freeze contains 438,060 candidate-strategy rows
(219,030 per namespace), preserves the source
Session 9 fold/label/feature/preprocessing/model/calibration identifiers for
every declared score role, and marks every row with a path-specific unavailable
exclusion. Holding tables are present but empty; no period has a selected row or
weight.

The two namespaces remain economically identical but physically separate.
`include_policy_imputed` added no selection-only rows, so the row/exclusion
identity hashes match without combining the files.

## Explicitly not performed

No model was fitted, calibrated, rescored, or thresholded. No market data was
sourced, no liquidity value was invented, no backtest or performance metric was
run, Session 9C was not started, and no commit or push occurred.
