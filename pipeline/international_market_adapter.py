"""Fail-closed, contract-only adapter for the first I1 international market.

I1 deliberately stops at source/P2 compatibility.  It does not refresh data,
recompute targets, fit models, build portfolios, or calculate performance.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


TORONTO = ZoneInfo("America/Toronto")


@dataclass(frozen=True)
class MarketContract:
    market: str
    country: str
    exchanges: tuple[str, ...]
    native_currency: str
    accounting_basis: str
    benchmark_id: str
    filing_timestamp_policy: str
    decision_clock: str
    target_policy: str
    purge_policy: str
    liquidity_policy: str
    cost_policy: str
    portfolio_policy: str


CA_CONTRACT = MarketContract(
    market="CA",
    country="Canada",
    exchanges=("TSX", "TSXV"),
    native_currency="CAD",
    accounting_basis="IFRS",
    benchmark_id="S&P/TSX Composite Total Return (vintage required)",
    filing_timestamp_policy=(
        "filed_date normalized to end-of-day America/Toronto; intraday "
        "publication time is unproven and cannot order same-day filings"
    ),
    decision_clock=(
        "first common TSX/TSXV regular session strictly after availability; "
        "calendar evidence required at execution"
    ),
    target_policy=(
        "observed-only native-CAD stock and local-benchmark returns; no FX "
        "conversion unless a dated FX observation is preserved"
    ),
    purge_policy="label_end_date must be strictly before decision timestamp",
    liquidity_policy="native-CAD dollar volume with a complete pre-decision session window",
    cost_policy="actual-traded-notional costs recorded in CAD; no estimate in I1",
    portfolio_policy="separate CAD sleeve and local benchmark; no cross-market aggregation",
)


REQUIRED_SNAPSHOT_COLUMNS = {
    "ticker", "stock_code", "exchange", "market", "country", "currency",
    "accounting_std", "filed_date", "fiscal_year", "period_type",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def availability_timestamp(value: object) -> pd.Timestamp:
    date = pd.Timestamp(value).date()
    return pd.Timestamp(datetime.combine(date, time.max, tzinfo=TORONTO)).tz_convert("UTC")


def _stable_row_id(row: pd.Series, availability: pd.Timestamp) -> str:
    raw = "|".join(
        ["CA", str(row["exchange"]), str(row["stock_code"]),
         str(row["fiscal_year"]), str(row["period_type"]), availability.isoformat()]
    )
    return "ca_" + hashlib.sha256(raw.encode()).hexdigest()[:32]


def build_p2_compatibility(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Normalize legacy CA snapshots into a row-complete P2 compatibility table."""
    missing = REQUIRED_SNAPSHOT_COLUMNS - set(snapshots.columns)
    if missing:
        raise ValueError(f"CA source is missing required columns: {sorted(missing)}")
    frame = snapshots.copy()
    for col in ["ticker", "stock_code", "exchange", "market", "country", "currency", "accounting_std", "filed_date", "fiscal_year", "period_type"]:
        if frame[col].isna().any() or frame[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"CA source has incomplete required field: {col}")
    if not frame["market"].astype(str).eq("CA").all():
        raise ValueError("CA adapter received non-CA rows")
    if not frame["currency"].astype(str).eq("CAD").all():
        raise ValueError("CA source currency is not uniformly CAD")
    if not frame["accounting_std"].astype(str).eq("IFRS").all():
        raise ValueError("CA source accounting basis is not uniformly IFRS")
    availability = pd.to_datetime(frame["filed_date"], errors="coerce").map(availability_timestamp)
    if availability.isna().any():
        raise ValueError("CA source contains an invalid filing date")
    out = pd.DataFrame({
        "stable_row_id": [
            _stable_row_id(row, ts) for (_, row), ts in zip(frame.iterrows(), availability)
        ],
        "entity_id": "CA:" + frame["stock_code"].astype(str),
        "security_id": frame["stock_code"].astype(str),
        "ticker": frame["ticker"].astype(str),
        "exchange": frame["exchange"].astype(str),
        "market": "CA",
        "country": "Canada",
        "currency": "CAD",
        "accounting_std": "IFRS",
        "fiscal_year": pd.to_numeric(frame["fiscal_year"], errors="raise").astype(int),
        "period_type": frame["period_type"].astype(str),
        "availability_timestamp": availability,
        "decision_timestamp": pd.NaT,
        "prediction_timestamp": pd.NaT,
        "label_end_date": pd.NaT,
        "benchmark_id": CA_CONTRACT.benchmark_id,
        "target_status": "unsupported_until_local_calendar_benchmark_action_evidence",
    })
    if out["stable_row_id"].duplicated().any():
        raise ValueError("CA P2 stable row IDs are not unique")
    return out.sort_values(["availability_timestamp", "security_id", "fiscal_year"]).reset_index(drop=True)


def compatibility_summary(p2: pd.DataFrame, source_paths: list[Path]) -> dict:
    """Return compatibility evidence without producing any target or performance value."""
    required = ["stable_row_id", "entity_id", "availability_timestamp", "market", "currency", "accounting_std"]
    row_complete = int(p2[required].notna().all(axis=1).sum()) == len(p2)
    return {
        "market": "CA",
        "contract": asdict(CA_CONTRACT),
        "source_rows": len(p2),
        "p2_core_row_complete": row_complete,
        "p2_core_rows": len(p2) if row_complete else int(p2[required].notna().all(axis=1).sum()),
        "p2_p3_targets": "unsupported_fail_closed",
        "p2_p4_calendar_benchmark_actions": "unsupported_fail_closed",
        "performance_calculated": False,
        "source_lineage": [
            {"path": str(path), "size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in source_paths
        ],
    }


def write_i1_artifact(root: Path, snapshots_path: Path, tickers_path: Path, prices_path: Path) -> dict:
    """Materialize one fresh non-overwriting I1 compatibility artifact."""
    if root.exists():
        existing = {path.name for path in root.iterdir()}
        if existing != {"preflight.json"}:
            raise FileExistsError(f"I1 artifact target is non-empty: {root}")
    else:
        root.mkdir(parents=True, exist_ok=False)
    snapshots = pd.read_parquet(snapshots_path)
    p2 = build_p2_compatibility(snapshots)
    p2_path = root / "p2_compatibility.parquet"
    p2.to_parquet(p2_path, index=False)
    summary = compatibility_summary(p2, [tickers_path, snapshots_path, prices_path])
    (root / "contract.json").write_text(json.dumps(asdict(CA_CONTRACT), indent=2, sort_keys=True) + "\n")
    (root / "compatibility_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    records = []
    for path in sorted(root.iterdir()):
        records.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    manifest = {
        "artifact_class": "I1_CA_ADAPTER_COMPATIBILITY",
        "session": "I1",
        "status": "compatibility_only_fail_closed",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "records": records,
        "outputs": {"source_rows": len(p2), "p2_core_row_complete": summary["p2_core_row_complete"], "performance_calculated": False},
        "unsupported": ["P2-P4 target/calendar/benchmark/action completeness", "P3 model compatibility", "P4 portfolio compatibility", "all historical performance"],
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"artifact_root": str(root), "manifest_sha256": sha256_file(manifest_path), "source_rows": len(p2)}


def refresh_manifest(root: Path) -> str:
    """Rebuild the manifest after adding independently captured evidence."""
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("manifest_sha256", None)
    manifest["records"] = []
    for path in sorted(root.iterdir()):
        if path.name == "manifest.json":
            continue
        manifest["records"].append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return sha256_file(manifest_path)
