"""
validate_feature_contract.py — Validate dataset column groups by pipeline phase.

Checks which column GROUPS are present in historical_dataset_clean.parquet and
reports whether Phase B and/or Phase C are complete.

Phase B = Steps 1-6 + enrichment mutators (universe, fraud, quarterly, imputation).
Phase C = ML scoring layer (OOF scores, ML scores, alpha factors, vol patches).

Design: group-level checks (not individual columns). A group is "present" if at
least N representative columns from that group exist. This avoids brittleness
when individual columns are added/removed.

Usage:
    python3 scripts/validate_feature_contract.py
    python3 scripts/validate_feature_contract.py --parquet data/historical_dataset_clean.parquet
    python3 scripts/validate_feature_contract.py --strict   # exit 1 if Phase C incomplete
    python3 scripts/validate_feature_contract.py --json     # machine-readable output

Exit codes:
    0 = current phase is internally consistent (Phase B complete, Phase C may be pending)
    1 = Phase B incomplete (structural problem) OR --strict and Phase C incomplete
"""
from __future__ import annotations

import argparse
import json as json_mod
import sys
from pathlib import Path

import pandas as pd
from scripts._root import ROOT

BASE = ROOT

DEFAULT_PARQUET = BASE / "data" / "historical_dataset_clean.parquet"


# ─── Column Group Definitions ────────────────────────────────────────────────
# Each group: (name, required_min_count, representative_columns, phase, source)
# A group passes if >= required_min_count of its representative columns exist.

COLUMN_GROUPS = [
    # ── Phase B: Core pipeline (Steps 1-6) ──
    {
        "name": "identifiers",
        "phase": "B",
        "source": "step1/step2",
        "min_present": 10,
        "columns": [
            "cik", "ticker", "name", "filed_date", "fiscal_year",
            "fiscal_quarter", "period_type", "exchange", "sic_code",
            "sic_description", "market", "country", "accounting_std",
            "size_category_label", "corp_code", "acc_mt",
        ],
    },
    {
        "name": "raw_financials",
        "phase": "B",
        "source": "step2",
        "min_present": 20,
        "columns": [
            "revenue", "net_income", "gross_profit", "operating_income",
            "total_assets", "total_equity", "current_assets",
            "current_liabilities", "operating_cash_flow", "long_term_debt",
            "short_term_debt", "total_debt", "cash", "capex", "fcf",
            "accounts_receivable", "inventory", "ppe_net", "goodwill",
            "intangibles", "market_cap_at_filing", "common_shares_outstanding",
            "eps_diluted", "tax_expense", "interest_expense",
            "retained_earnings", "additional_paid_in_capital",
        ],
    },
    {
        "name": "price_momentum",
        "phase": "B",
        "source": "step3",
        "min_present": 8,
        "columns": [
            "entry_price", "entry_date", "forward_return_1y",
            "forward_return_3y", "forward_return_5y",
            "beat_local_market_1y", "beat_local_market_3y",
            "momentum_3m_prior", "momentum_6m_prior", "momentum_12m_prior",
            "volatility_12m", "high_52w_pct",
        ],
    },
    {
        "name": "macro",
        "phase": "B",
        "source": "step4",
        "min_present": 8,
        "columns": [
            "treasury_10y", "treasury_2y", "yield_curve", "fed_funds_rate",
            "credit_spread_baa", "hy_spread", "cpi_yoy", "recession",
            "vix", "real_rate_10y", "credit_tightening", "macro_regime",
        ],
    },
    {
        "name": "computed_features",
        "phase": "B",
        "source": "step5",
        "min_present": 25,
        "columns": [
            # Valuation (step5 group A)
            "pe_ratio", "ev_ebitda", "fcf_yield", "earnings_yield", "ev_revenue",
            # Profitability (step5 group B)
            "roe", "gross_margin", "net_margin", "operating_margin", "roic",
            # Fraud/distress scores (step5 group D)
            "altman_z_score", "beneish_m_score", "piotroski_f_score",
            "montier_c1", "montier_c2", "sloan_accruals",
            # Leverage (step5 group D)
            "current_ratio", "debt_to_equity", "debt_to_assets", "interest_coverage",
            # Growth (step5 group E — from step2 YoY)
            "revenue_growth_yoy", "net_income_growth_yoy", "asset_growth_yoy",
            # Efficiency
            "asset_turnover", "noa_growth",
            # Interaction features (step5 group H)
            "quality_composite", "value_composite",
            # Size
            "size_category",
            # Beneish sub-components
            "beneish_dsri", "beneish_gmi", "beneish_aqi", "beneish_sgi",
        ],
    },
    # ── Phase B: Enrichment mutators (post-step6) ──
    {
        "name": "universe_confidence",
        "phase": "B",
        "source": "p0f + p0g",
        "min_present": 3,
        "columns": [
            "in_universe", "excl_reason", "data_confidence",
        ],
    },
    {
        "name": "fraud_labels",
        "phase": "B",
        "source": "enrich_fraud_labels + fetch_aaer_labels",
        "min_present": 2,
        "columns": [
            "fraud_confirmed", "fraud_suspect",
        ],
    },
    {
        "name": "fraud_taxonomy",
        "phase": "B",
        "source": "enrich_fraud_taxonomy",
        "min_present": 5,
        "columns": [
            "fraud_score_accounting", "fraud_score_dilution",
            "fraud_score_quality", "fraud_score_distress",
            "fraud_score_governance", "fraud_score_composite",
        ],
    },
    # ── Phase C: ML scoring layer ──
    {
        "name": "oof_scores",
        "phase": "C",
        "source": "generate_oof_scores.py",
        "min_present": 3,
        "columns": [
            "ml_6m_oof", "ml_1y_oof", "ml_2y_oof", "ml_3y_oof", "ml_5y_oof",
        ],
    },
    {
        "name": "ml_scores",
        "phase": "C",
        "source": "score_historical.py",
        "min_present": 3,
        "columns": [
            "ml_6m", "ml_1y", "ml_2y", "ml_3y", "ml_5y", "ml_pred_excess_3y",
        ],
    },
    {
        "name": "alpha_factors",
        "phase": "C",
        "source": "compute_alpha.py",
        "min_present": 5,
        "columns": [
            "alpha_value", "alpha_quality", "alpha_momentum",
            "alpha_growth", "alpha_fraud_risk", "alpha_composite",
        ],
    },
    {
        "name": "vol_patches",
        "phase": "C",
        "source": "patch_equity_vol_features.py",
        "min_present": 3,
        "columns": [
            "equity_vol_6m", "equity_vol_12m", "equity_vol_36m",
            "equity_vol_60m", "roa_vol_5y",
        ],
    },
    # ── Phase C: Enrichment that requires model outputs or full pipeline ──
    {
        "name": "survivorship",
        "phase": "C",
        "source": "mark_survivorship.py",
        "min_present": 1,
        "columns": [
            "delisted_flag",
        ],
    },
    {
        "name": "quarterly_enriched",
        "phase": "C",
        "source": "enrich_quarterly_features.py",
        "min_present": 2,
        "columns": [
            "revenue_qoq_std", "earnings_momentum", "filing_lag_trend",
        ],
    },
]


# ─── Validation Logic ─────────────────────────────────────────────────────────

def validate_groups(df_columns: set[str]) -> list[dict]:
    """Check each column group and return results."""
    results = []
    for group in COLUMN_GROUPS:
        present = [c for c in group["columns"] if c in df_columns]
        missing = [c for c in group["columns"] if c not in df_columns]
        passes = len(present) >= group["min_present"]
        results.append({
            "name": group["name"],
            "phase": group["phase"],
            "source": group["source"],
            "present": len(present),
            "total": len(group["columns"]),
            "min_required": group["min_present"],
            "passes": passes,
            "missing": missing,
        })
    return results


def check_unexpected_columns(df_columns: set[str]) -> list[str]:
    """Find columns not covered by any group definition or known patterns.

    Returns only truly surprising columns. Most feature columns are expected
    but not individually enumerated (the contract validates groups, not individuals).
    """
    all_known = set()
    for group in COLUMN_GROUPS:
        all_known.update(group["columns"])

    # Additional known columns not in groups
    known_extras = {
        "as_of_date", "filing_lag_days", "fraud_source", "fraud_label",
        "pretax_income", "cogs", "sga_expense", "rd_expense",
        "depreciation", "da_expense", "financing_cash_flow",
        "investing_cash_flow", "accounts_payable", "receivables", "noa",
        "eps_basic", "benchmark_used", "currency", "stock_code",
        "industry_code", "sic_2digit", "likely_delisted",
    }
    all_known.update(known_extras)

    # Known feature name patterns — these are expected computed features
    KNOWN_PATTERNS = (
        "forward_return_", "beat_local_market_", "excess_return_",
        "benchmark_return_", "momentum_", "sector_rel_",
        "revenue_growth_", "net_income_growth_", "margin_",
        "piotroski_", "beneish_", "altman_", "ohlson_", "montier_",
        "sloan_", "accruals_", "quality_", "value_",
    )
    KNOWN_SUBSTRINGS = (
        "_yoy", "_3y_trend", "_rank", "_composite", "_sector_pct",
        "_x_", "_in_recession", "_pct", "_ratio", "_change",
        "_to_", "_growth", "interaction_", "_trend_", "_stability",
        "_intensity", "_conversion", "_leverage", "_margin",
        "_vol_", "vol_prior_", "_cagr_", "log_", "days_",
        "delta_", "earnings_", "revenue_", "operating_",
        "gross_", "debt_", "cash_", "equity_", "shares_",
        "size_category", "roa", "roe", "roic", "ebitda",
        "capex", "fcf", "ocf", "cfo", "cfi", "ev_", "ppe",
        "sga", "rd_", "dividends", "denom", "non_operating",
        "leverage", "total_liabilities", "quarterly_",
        "other_noncurrent", "depreciation", "net_debt",
    )

    unexpected = []
    for col in sorted(df_columns - all_known):
        if any(col.startswith(p) for p in KNOWN_PATTERNS):
            continue
        if any(kw in col for kw in KNOWN_SUBSTRINGS):
            continue
        unexpected.append(col)

    return unexpected


def run_validation(parquet_path: Path, strict: bool = False, as_json: bool = False) -> int:
    """Run full validation. Returns exit code."""
    if not parquet_path.exists():
        print(f"ERROR: {parquet_path} not found")
        return 1

    import pyarrow.parquet as pq
    pf = pq.ParquetFile(parquet_path)
    df_cols = set(pf.schema.names)
    row_count = pf.metadata.num_rows

    results = validate_groups(df_cols)
    unexpected = check_unexpected_columns(df_cols)

    phase_b_groups = [r for r in results if r["phase"] == "B"]
    phase_c_groups = [r for r in results if r["phase"] == "C"]

    phase_b_complete = all(r["passes"] for r in phase_b_groups)
    phase_c_complete = all(r["passes"] for r in phase_c_groups)

    phase_b_passing = sum(1 for r in phase_b_groups if r["passes"])
    phase_c_passing = sum(1 for r in phase_c_groups if r["passes"])

    if as_json:
        output = {
            "parquet": str(parquet_path),
            "rows": row_count,
            "columns": len(df_cols),
            "phase_b_complete": phase_b_complete,
            "phase_c_complete": phase_c_complete,
            "groups": results,
            "unexpected_columns": unexpected,
        }
        print(json_mod.dumps(output, indent=2))
    else:
        print(f"Feature Contract Validation")
        print(f"{'='*60}")
        print(f"Dataset: {parquet_path.name}")
        print(f"Shape:   {row_count:,} rows x {len(df_cols)} columns")
        print()

        print(f"Phase B (pipeline + enrichment): "
              f"{'COMPLETE' if phase_b_complete else 'INCOMPLETE'} "
              f"({phase_b_passing}/{len(phase_b_groups)} groups)")
        print(f"{'-'*60}")
        for r in phase_b_groups:
            status = "PASS" if r["passes"] else "FAIL"
            print(f"  [{status}] {r['name']:25s} "
                  f"{r['present']:3d}/{r['total']:3d} cols "
                  f"(min {r['min_required']}) <- {r['source']}")
            if not r["passes"] and r["missing"]:
                print(f"         missing: {', '.join(r['missing'][:5])}"
                      f"{'...' if len(r['missing']) > 5 else ''}")

        print()
        print(f"Phase C (ML scoring + patches): "
              f"{'COMPLETE' if phase_c_complete else 'PENDING'} "
              f"({phase_c_passing}/{len(phase_c_groups)} groups)")
        print(f"{'-'*60}")
        for r in phase_c_groups:
            status = "PASS" if r["passes"] else "----"
            print(f"  [{status}] {r['name']:25s} "
                  f"{r['present']:3d}/{r['total']:3d} cols "
                  f"(min {r['min_required']}) <- {r['source']}")

        if unexpected:
            print(f"\nUnexpected columns ({len(unexpected)}):")
            for col in unexpected[:10]:
                print(f"  ? {col}")
            if len(unexpected) > 10:
                print(f"  ... and {len(unexpected) - 10} more")

        print()
        if phase_b_complete and phase_c_complete:
            print("Result: ALL PHASES COMPLETE")
        elif phase_b_complete:
            print("Result: Phase B COMPLETE, Phase C PENDING (expected before model retrain)")
        else:
            print("Result: Phase B INCOMPLETE — structural issue, investigate")

    # Exit code logic
    if not phase_b_complete:
        return 1
    if strict and not phase_c_complete:
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description="Validate feature contract by pipeline phase")
    parser.add_argument("--parquet", default=str(DEFAULT_PARQUET))
    parser.add_argument("--strict", action="store_true",
                        help="Exit 1 if Phase C incomplete")
    parser.add_argument("--json", action="store_true",
                        help="Machine-readable JSON output")
    args = parser.parse_args()

    exit_code = run_validation(Path(args.parquet), strict=args.strict, as_json=args.json)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
