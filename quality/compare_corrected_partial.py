"""Create bounded, machine-readable LEGACY_SAVED/CORRECTED_PARTIAL differences."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


KEY = ["cik", "ticker", "filed_date", "fiscal_year", "fiscal_quarter", "period_type"]
HORIZONS = ("6m", "1y", "2y", "3y", "4y", "5y", "6y", "7y", "8y", "10y", "15y")


def _key_frame(df: pd.DataFrame) -> pd.DataFrame:
    key = df[KEY].copy()
    key["filed_date"] = pd.to_datetime(key["filed_date"], errors="coerce")
    return key


def _same(left: pd.Series, right: pd.Series) -> np.ndarray:
    if pd.api.types.is_numeric_dtype(left.dtype) and pd.api.types.is_numeric_dtype(right.dtype):
        return np.isclose(
            pd.to_numeric(left, errors="coerce").to_numpy(dtype=float),
            pd.to_numeric(right, errors="coerce").to_numpy(dtype=float),
            rtol=1e-12,
            atol=1e-12,
            equal_nan=True,
        )
    if pd.api.types.is_datetime64_any_dtype(left.dtype) or pd.api.types.is_datetime64_any_dtype(right.dtype):
        lval = pd.to_datetime(left, errors="coerce")
        rval = pd.to_datetime(right, errors="coerce")
        return ((lval == rval) | (lval.isna() & rval.isna())).to_numpy()
    lval = left.astype("string")
    rval = right.astype("string")
    return ((lval == rval) | (lval.isna() & rval.isna())).fillna(False).to_numpy()


def _label_summary(dataset: str, df: pd.DataFrame) -> list[dict]:
    records: list[dict] = []
    for horizon in HORIZONS:
        stock_target = f"forward_return_{horizon}"
        relative_target = f"beat_local_market_{horizon}"
        stock_date = f"stock_label_end_date_{horizon}"
        relative_date = f"label_end_date_{horizon}"
        stock_prov = f"stock_label_provenance_{horizon}"
        relative_prov = f"label_provenance_{horizon}"
        policy_stock_date = f"policy_stock_label_available_date_{horizon}"
        policy_relative_date = f"policy_label_available_date_{horizon}"

        def present(column: str) -> pd.Series:
            return df[column].notna() if column in df else pd.Series(False, index=df.index)

        observed_stock = (
            present(stock_target)
            & present(stock_date)
            & (df[stock_prov].eq("observed_market_price") if stock_prov in df else False)
        )
        observed_relative = (
            present(relative_target)
            & present(relative_date)
            & (df[relative_prov].eq("observed_stock_and_benchmark_prices") if relative_prov in df else False)
        )
        policy_stock = (
            present(stock_target)
            & present(policy_stock_date)
            & (df[stock_prov].eq("policy_imputed_likely_delisted") if stock_prov in df else False)
        )
        policy_relative = (
            present(relative_target)
            & present(policy_relative_date)
            & (df[relative_prov].eq("policy_imputed_likely_delisted") if relative_prov in df else False)
        )
        records.append({
            "dataset": dataset,
            "horizon": horizon,
            "stock_target_non_null": int(present(stock_target).sum()),
            "relative_target_non_null": int(present(relative_target).sum()),
            "observed_stock_structurally_eligible": int(observed_stock.sum()),
            "observed_relative_structurally_eligible": int(observed_relative.sum()),
            "policy_stock_sensitivity_eligible": int(policy_stock.sum()),
            "policy_relative_sensitivity_eligible": int(policy_relative.sum()),
            "stock_date_non_null": int(present(stock_date).sum()),
            "relative_date_non_null": int(present(relative_date).sum()),
        })
    return records


def compare(legacy_path: Path, corrected_path: Path, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    legacy = pd.read_parquet(legacy_path)
    corrected = pd.read_parquet(corrected_path)

    counts = []
    for dataset, df in (("LEGACY_SAVED", legacy), ("CORRECTED_PARTIAL", corrected)):
        grouped = (
            df.groupby(["fiscal_year", "market", "period_type"], dropna=False)
            .size().rename("rows").reset_index()
        )
        grouped.insert(0, "dataset", dataset)
        counts.append(grouped)
    pd.concat(counts, ignore_index=True).to_csv(output_dir / "row_counts_by_year_market.csv", index=False)

    filing_records = []
    for dataset, df in (("LEGACY_SAVED", legacy), ("CORRECTED_PARTIAL", corrected)):
        filing = pd.DataFrame({
            "market": df["market"],
            "filed_date": pd.to_datetime(df["filed_date"], errors="coerce"),
        })
        for market, group in filing.groupby("market", dropna=False):
            filing_records.append({
                "dataset": dataset,
                "market": market,
                "rows": len(group),
                "null_filing_dates": int(group["filed_date"].isna().sum()),
                "unique_filing_dates": int(group["filed_date"].nunique()),
                "min_filing_date": group["filed_date"].min(),
                "max_filing_date": group["filed_date"].max(),
            })
    pd.DataFrame(filing_records).to_csv(output_dir / "filing_date_summary.csv", index=False)

    legacy_key = _key_frame(legacy)
    corrected_key = _key_frame(corrected)
    key_diff = legacy_key.merge(corrected_key, on=KEY, how="outer", indicator=True)
    key_diff["difference"] = key_diff["_merge"].map({
        "left_only": "removed_from_corrected",
        "right_only": "added_in_corrected",
        "both": "common",
    })
    key_diff = key_diff[key_diff["difference"] != "common"].drop(columns=["_merge"])
    key_diff["explanation"] = np.where(
        (key_diff["difference"] == "added_in_corrected") & key_diff["period_type"].eq("quarterly"),
        "expected_broader_frozen_step2_period_scope",
        "unresolved_annual_universe_difference",
    )
    key_diff.to_csv(output_dir / "row_key_differences.csv", index=False)

    schema_records = []
    for column in sorted(set(legacy.columns) | set(corrected.columns)):
        l_present = column in legacy
        c_present = column in corrected
        schema_records.append({
            "column": column,
            "legacy_present": l_present,
            "corrected_present": c_present,
            "legacy_dtype": str(legacy[column].dtype) if l_present else None,
            "corrected_dtype": str(corrected[column].dtype) if c_present else None,
            "legacy_missing": int(legacy[column].isna().sum()) if l_present else None,
            "corrected_missing": int(corrected[column].isna().sum()) if c_present else None,
            "legacy_missing_pct": float(legacy[column].isna().mean()) if l_present else None,
            "corrected_missing_pct": float(corrected[column].isna().mean()) if c_present else None,
        })
    schema = pd.DataFrame(schema_records)

    legacy_common = legacy.merge(corrected_key, on=KEY, how="inner")
    corrected_common = corrected.merge(legacy_key, on=KEY, how="inner")
    legacy_common = legacy_common.sort_values(KEY, kind="stable").reset_index(drop=True)
    corrected_common = corrected_common.sort_values(KEY, kind="stable").reset_index(drop=True)
    if len(legacy_common) != len(corrected_common):
        raise RuntimeError("common-key alignment is not one-to-one")

    row_changed = np.zeros(len(legacy_common), dtype=bool)
    change_records = []
    for column in sorted((set(legacy.columns) & set(corrected.columns)) - set(KEY)):
        same = _same(legacy_common[column], corrected_common[column])
        changed = ~same
        row_changed |= changed
        change_records.append({
            "column": column,
            "common_rows": len(changed),
            "changed_rows": int(changed.sum()),
            "unchanged_rows": int(same.sum()),
            "classification": (
                "expected_corrected_label_lineage"
                if any(token in column for token in ("forward_return", "beat_local", "excess_return"))
                else "requires_review_corrected_transform_or_stale_snapshot_difference"
            ),
        })
    changes = pd.DataFrame(change_records).sort_values(["changed_rows", "column"], ascending=[False, True])
    changes.to_csv(output_dir / "shared_column_value_changes.csv", index=False)
    schema.merge(changes[["column", "changed_rows"]], on="column", how="left").to_csv(
        output_dir / "schema_missingness_and_changes.csv", index=False
    )

    pd.DataFrame(_label_summary("LEGACY_SAVED", legacy) + _label_summary("CORRECTED_PARTIAL", corrected)).to_csv(
        output_dir / "horizon_label_eligibility.csv", index=False
    )

    class_records = []
    class_columns = [c for c in corrected if c.startswith("beat_local_market_")]
    class_columns += [c for c in ("fraud_confirmed", "fraud_suspect") if c in legacy or c in corrected]
    for dataset, df in (("LEGACY_SAVED", legacy), ("CORRECTED_PARTIAL", corrected)):
        for column in class_columns:
            if column not in df:
                class_records.append({"dataset": dataset, "column": column, "value": "COLUMN_ABSENT", "rows": 0})
                continue
            for value, rows in df[column].value_counts(dropna=False).items():
                class_records.append({
                    "dataset": dataset,
                    "column": column,
                    "value": "NULL" if pd.isna(value) else str(value),
                    "rows": int(rows),
                })
    pd.DataFrame(class_records).to_csv(output_dir / "class_balance.csv", index=False)

    disappearing = corrected[corrected.get("likely_delisted", False).fillna(False)].copy()
    disappearing_summary = (
        disappearing.groupby("ticker", dropna=False)
        .agg(rows=("ticker", "size"), first_fiscal_year=("fiscal_year", "min"), last_fiscal_year=("fiscal_year", "max"))
        .reset_index()
    )
    disappearing_summary["remains_in_corrected_universe"] = True
    disappearing_summary.to_csv(output_dir / "disappearing_companies_remaining.csv", index=False)

    summary = {
        "labels": {
            "legacy": "LEGACY_SAVED",
            "corrected": "CORRECTED_PARTIAL",
            "legacy_reproduced": False,
        },
        "rows": {"legacy": len(legacy), "corrected": len(corrected), "common": len(legacy_common)},
        "columns": {
            "legacy": len(legacy.columns),
            "corrected": len(corrected.columns),
            "common": len(set(legacy.columns) & set(corrected.columns)),
            "legacy_only": len(set(legacy.columns) - set(corrected.columns)),
            "corrected_only": len(set(corrected.columns) - set(legacy.columns)),
        },
        "keys": {
            "fields": KEY,
            "legacy_unique": int(len(legacy_key.drop_duplicates(KEY))),
            "corrected_unique": int(len(corrected_key.drop_duplicates(KEY))),
            "legacy_duplicates": int(legacy_key.duplicated(KEY).sum()),
            "corrected_duplicates": int(corrected_key.duplicated(KEY).sum()),
            "added": int((key_diff["difference"] == "added_in_corrected").sum()),
            "removed": int((key_diff["difference"] == "removed_from_corrected").sum()),
            "unresolved_annual_differences": int((key_diff["explanation"] == "unresolved_annual_universe_difference").sum()),
        },
        "values": {
            "shared_columns_with_changes": int((changes["changed_rows"] > 0).sum()),
            "shared_columns_unchanged": int((changes["changed_rows"] == 0).sum()),
            "common_rows_with_any_change": int(row_changed.sum()),
            "common_rows_unchanged": int((~row_changed).sum()),
        },
        "disappearing_companies": {
            "tickers": len(disappearing_summary),
            "rows": len(disappearing),
            "all_retained": True,
        },
        "limitations": [
            "CORRECTED_PARTIAL uses stale pre-fix Step 2 snapshots.",
            "CORRECTED_PARTIAL is conditioned on the frozen incomplete daily cache.",
            "Structural eligibility is reported without a scoring cutoff because Session 8B calendars are unresolved.",
            "LEGACY_SAVED is evidence only and was not reproduced.",
        ],
    }
    (output_dir / "comparison_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy", type=Path, required=True)
    parser.add_argument("--corrected", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(compare(args.legacy, args.corrected, args.out_dir), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
