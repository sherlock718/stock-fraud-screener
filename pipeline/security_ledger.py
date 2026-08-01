"""Provider-neutral historical security and survivorship ledger.

The S1 builder is deliberately offline: it consumes already-versioned exact
source responses, preserves the selected raw bytes again below a new immutable
ledger version, and reports evidence gaps without inferring events or returns.
Provider adapters emit the same normalized tables so a paid source can later
supplement the free evidence without changing downstream contracts.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_D1_ROOT = (
    ROOT / "artifacts/canonical_refresh/us/20260730T110301Z"
)
DEFAULT_P4_ROOT = (
    ROOT / "artifacts/canonical/corrected_us_annual_3y_product"
)
LEDGER_ID_PATTERN = re.compile(r"^[0-9]{8}T[0-9]{6}Z(?:-[a-z0-9-]+)?$")
BENCHMARK_SYMBOLS = ("IWC", "IWM", "MDY", "SPY")

ISSUER_COLUMNS = (
    "issuer_id", "jurisdiction", "sec_cik", "legal_name", "entity_type",
    "effective_start", "effective_end", "effective_time_status",
    "source_published_at", "source_retrieved_at", "source_id",
    "source_response_sha256", "confidence", "ambiguity",
)
SECURITY_COLUMNS = (
    "security_id", "issuer_id", "security_type", "security_type_status",
    "share_class", "effective_start", "effective_end",
    "effective_time_status", "source_published_at", "source_retrieved_at",
    "source_id", "source_response_sha256", "confidence", "ambiguity",
)
LISTING_COLUMNS = (
    "listing_id", "security_id", "ticker", "exchange", "exchange_mic",
    "effective_start", "effective_end", "effective_time_status",
    "source_published_at", "source_retrieved_at", "source_id",
    "source_response_sha256", "confidence", "ambiguity",
)
EVENT_COLUMNS = (
    "event_id", "issuer_id", "security_id", "event_type", "event_status",
    "effective_at", "effective_time_status", "source_published_at",
    "source_retrieved_at", "source_id", "source_response_sha256",
    "accession_number", "form", "primary_document", "evidence_detail",
    "confidence", "ambiguity", "primary_return_available",
    "event_total_return",
)
COVERAGE_COLUMNS = (
    "requirement_id", "instrument_role", "stable_row_id", "issuer_id",
    "security_id", "ticker", "required_start", "required_end",
    "coverage_status", "identity_status", "listing_status",
    "security_type_status", "event_status", "price_adjustment_status",
    "reason_codes", "source_ids",
)
RAW_EVIDENCE_COLUMNS = (
    "source_id", "provider", "request_url", "source_published_at",
    "source_retrieved_at", "response_size_bytes", "response_sha256",
    "stored_size_bytes", "stored_sha256", "source_stored_path",
    "ledger_stored_path",
)

PRICE_ADJUSTMENT_CONTRACT = {
    "schema_version": 1,
    "execution_price": (
        "Use an unadjusted regular-session close for actual entry/exit "
        "notional when that observation is explicitly sourced."
    ),
    "total_return_price": (
        "Use an adjusted close only when provider metadata explicitly states "
        "which splits and cash distributions are embedded."
    ),
    "double_counting": (
        "Never post a split or distribution again when it is already embedded "
        "in the selected adjusted series."
    ),
    "non_price_consideration": (
        "Merger consideration, successor shares, liquidation recovery, cash "
        "in lieu, and other unembedded value require dated action terms."
    ),
    "missing_or_ambiguous": (
        "A missing price, uncertain adjustment basis, conflicting identity, "
        "or unresolved action leaves the affected primary outcome unavailable."
    ),
    "delisting_return": (
        "A delisting or disappearance is not assigned a primary return unless "
        "the observed price/action evidence supports the complete outcome."
    ),
}

SENSITIVITY_CONTRACT = {
    "schema_version": 1,
    "scenario_id": "legacy_minus_50_percent_unsupported_exit",
    "scenario_class": "policy_sensitivity_only",
    "assumed_return": -0.50,
    "applies_to": "unsupported_exit_only_when_explicitly_selected",
    "physical_namespace": "sensitivity/",
    "prohibited_destinations": [
        "outputs/primary/",
        "observed_labels",
        "model_training",
        "official_performance",
    ],
    "observed_fact": False,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_id(prefix: str, *values: object) -> str:
    text = "\x1f".join("" if value is None else str(value) for value in values)
    return f"{prefix}:{hashlib.sha256(text.encode()).hexdigest()[:24]}"


def _read_json_gzip(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rb") as handle:
        return json.loads(handle.read())


def _latest_response_records(path: Path) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        item = json.loads(line)
        key = str(item["logical_key"])
        if key in latest and latest[key].get("status") == "success":
            raise RuntimeError(
                f"response follows terminal success for {key} at line {number}"
            )
        latest[key] = item
    return latest


def _verify_response(source_root: Path, item: dict[str, Any]) -> bytes:
    path = source_root / str(item["stored_path"])
    if (
        not path.is_file()
        or path.stat().st_size != int(item["stored_size_bytes"])
        or sha256_file(path) != item["stored_sha256"]
    ):
        raise RuntimeError(f"stored source response mismatch: {path}")
    with gzip.open(path, "rb") as handle:
        payload = handle.read()
    if (
        len(payload) != int(item["response_size_bytes"])
        or hashlib.sha256(payload).hexdigest() != item["response_sha256"]
    ):
        raise RuntimeError(f"decompressed source response mismatch: {path}")
    return payload


def _published_at(item: dict[str, Any]) -> str | None:
    headers = item.get("response_headers", {})
    return headers.get("Last-Modified") or headers.get("Date")


def _mic(exchange: str | None) -> str | None:
    return {
        "Nasdaq": "XNAS",
        "NYSE": "XNYS",
        "NYSE American": "XASE",
        "Cboe BZX": "BATS",
    }.get(str(exchange))


class SecurityLedgerAdapter(Protocol):
    """Boundary implemented by free and future paid evidence providers."""

    provider_name: str

    def normalize(
        self,
        required_instruments: pd.DataFrame,
    ) -> dict[str, pd.DataFrame]:
        """Return issuer, security, listing, event, and raw-evidence tables."""


@dataclass
class SecSubmissionAdapter:
    """Normalize exact SEC index/submission payloads already frozen by D1."""

    source_root: Path
    ledger_root: Path
    provider_name: str = "sec_edgar"

    def _copy_raw(
        self,
        item: dict[str, Any],
        raw_records: list[dict[str, Any]],
    ) -> bytes:
        payload = _verify_response(self.source_root, item)
        source_path = self.source_root / str(item["stored_path"])
        target_relative = Path("raw/sec") / str(item["stored_path"]).removeprefix(
            "raw/"
        )
        target = self.ledger_root / target_relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise RuntimeError(f"ledger raw target already exists: {target}")
        shutil.copyfile(source_path, target)
        if (
            target.stat().st_size != int(item["stored_size_bytes"])
            or sha256_file(target) != item["stored_sha256"]
        ):
            raise RuntimeError(f"copied ledger raw response mismatch: {target}")
        raw_records.append(
            {
                "source_id": item["logical_key"],
                "provider": self.provider_name,
                "request_url": item["request_url"],
                "source_published_at": _published_at(item),
                "source_retrieved_at": item["retrieved_at_utc"],
                "response_size_bytes": item["response_size_bytes"],
                "response_sha256": item["response_sha256"],
                "stored_size_bytes": item["stored_size_bytes"],
                "stored_sha256": item["stored_sha256"],
                "source_stored_path": str(item["stored_path"]),
                "ledger_stored_path": target_relative.as_posix(),
            }
        )
        return payload

    def normalize(
        self,
        required_instruments: pd.DataFrame,
    ) -> dict[str, pd.DataFrame]:
        response_manifest = self.source_root / "raw/response_manifest.jsonl"
        records = _latest_response_records(response_manifest)
        raw_records: list[dict[str, Any]] = []
        ticker_payload = json.loads(
            self._copy_raw(records["company_tickers"], raw_records)
        )
        exchange_payload = json.loads(
            self._copy_raw(records["company_tickers_exchange"], raw_records)
        )
        exchange_rows = {
            (str(row[0]).zfill(10), str(row[2]).upper()): row
            for row in exchange_payload.get("data", [])
        }
        ticker_rows = {
            (str(item["cik_str"]).zfill(10), str(item["ticker"]).upper()): item
            for item in ticker_payload.values()
        }

        issuers: list[dict[str, Any]] = []
        securities: list[dict[str, Any]] = []
        listings: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for required in required_instruments.itertuples(index=False):
            cik = str(required.sec_cik).zfill(10)
            ticker = str(required.ticker).upper()
            key = (cik, ticker)
            if key in seen:
                continue
            seen.add(key)
            source_id = f"submission_{cik}"
            item = records.get(source_id)
            if item is None or item.get("status") != "success":
                continue
            submission = json.loads(self._copy_raw(item, raw_records))
            issuer_id = f"issuer:us-sec:{cik}"
            security_id = _hash_id("security", issuer_id, ticker)
            retrieved = item["retrieved_at_utc"]
            published = _published_at(item)
            current_tickers = [
                str(value).upper() for value in submission.get("tickers", [])
            ]
            current_exchanges = [
                str(value) for value in submission.get("exchanges", [])
            ]
            index_row = exchange_rows.get(key)
            index_item = ticker_rows.get(key)
            association_pairs = set(zip(current_tickers, current_exchanges))
            exchange = str(index_row[3]) if index_row else None
            current_pair_supported = any(
                pair[0] == ticker for pair in association_pairs
            )
            conflict = bool(
                exchange
                and current_pair_supported
                and not any(
                    pair[0] == ticker and pair[1] == exchange
                    for pair in association_pairs
                )
            )
            ambiguity = (
                "conflicting_current_sec_association"
                if conflict
                else "current_snapshot_has_no_historical_effective_start"
            )
            issuers.append(
                {
                    "issuer_id": issuer_id,
                    "jurisdiction": "US",
                    "sec_cik": cik,
                    "legal_name": submission.get("name")
                    or (index_item or {}).get("title"),
                    "entity_type": submission.get("entityType"),
                    "effective_start": pd.NaT,
                    "effective_end": pd.NaT,
                    "effective_time_status": "unavailable_current_snapshot",
                    "source_published_at": published,
                    "source_retrieved_at": retrieved,
                    "source_id": source_id,
                    "source_response_sha256": item["response_sha256"],
                    "confidence": "high_current_identity",
                    "ambiguity": ambiguity,
                }
            )
            securities.append(
                {
                    "security_id": security_id,
                    "issuer_id": issuer_id,
                    "security_type": pd.NA,
                    "security_type_status": "unsupported",
                    "share_class": ticker,
                    "effective_start": pd.NaT,
                    "effective_end": pd.NaT,
                    "effective_time_status": "unavailable_current_snapshot",
                    "source_published_at": published,
                    "source_retrieved_at": retrieved,
                    "source_id": source_id,
                    "source_response_sha256": item["response_sha256"],
                    "confidence": "low_security_identity",
                    "ambiguity": (
                        "sec_cik_and_ticker_do_not_prove_permanent_security_or_type"
                    ),
                }
            )
            listings.append(
                {
                    "listing_id": _hash_id(
                        "listing", security_id, ticker, exchange
                    ),
                    "security_id": security_id,
                    "ticker": ticker,
                    "exchange": exchange,
                    "exchange_mic": _mic(exchange),
                    "effective_start": pd.NaT,
                    "effective_end": pd.NaT,
                    "effective_time_status": "unavailable_current_snapshot",
                    "source_published_at": published,
                    "source_retrieved_at": retrieved,
                    "source_id": source_id,
                    "source_response_sha256": item["response_sha256"],
                    "confidence": (
                        "conflicting" if conflict else "high_current_only"
                    ),
                    "ambiguity": ambiguity,
                }
            )
            recent = submission.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            for index, form in enumerate(forms):
                items = str(_at(recent, "items", index) or "")
                is_bankruptcy = form in {"8-K", "8-K/A"} and (
                    "1.03" in {value.strip() for value in items.split(",")}
                )
                if (
                    form not in {
                        "25", "25-NSE", "15-12B", "15-12G", "15-15D"
                    }
                    and not is_bankruptcy
                ):
                    continue
                accession = _at(recent, "accessionNumber", index)
                accepted = _at(recent, "acceptanceDateTime", index)
                filed = _at(recent, "filingDate", index)
                events.append(
                    {
                        "event_id": _hash_id(
                            "event", cik, accession, form
                        ),
                        "issuer_id": issuer_id,
                        "security_id": security_id,
                        "event_type": (
                            "bankruptcy"
                            if is_bankruptcy
                            else (
                                "delisting"
                                if form in {"25", "25-NSE"}
                                else "registration_termination"
                            )
                        ),
                        "event_status": "filing_indicator_unresolved",
                        "effective_at": pd.NaT,
                        "effective_time_status": (
                            "not_stated_in_submission_index"
                        ),
                        "source_published_at": accepted or filed,
                        "source_retrieved_at": retrieved,
                        "source_id": source_id,
                        "source_response_sha256": item["response_sha256"],
                        "accession_number": accession,
                        "form": form,
                        "primary_document": _at(
                            recent, "primaryDocument", index
                        ),
                        "evidence_detail": (
                            "SEC submission index proves the filing"
                            + (
                                " and explicitly reports Item 1.03"
                                if is_bankruptcy
                                else ""
                            )
                            + ", but not complete event terms or return."
                        ),
                        "confidence": "high_filing_low_resolution",
                        "ambiguity": (
                            "effective_time_and_security_scope_require_primary_document"
                        ),
                        "primary_return_available": False,
                        "event_total_return": pd.NA,
                    }
                )

        return {
            "issuers": pd.DataFrame(issuers, columns=ISSUER_COLUMNS),
            "securities": pd.DataFrame(
                securities, columns=SECURITY_COLUMNS
            ),
            "listings": pd.DataFrame(listings, columns=LISTING_COLUMNS),
            "events": pd.DataFrame(events, columns=EVENT_COLUMNS),
            "raw_evidence": pd.DataFrame(
                raw_records, columns=RAW_EVIDENCE_COLUMNS
            ),
        }


def _at(document: dict[str, list[Any]], key: str, index: int) -> Any:
    values = document.get(key, [])
    return values[index] if index < len(values) else None


def required_instruments(
    p4_root: Path,
    exchange_index_path: Path,
) -> pd.DataFrame:
    holdings = pd.read_parquet(p4_root / "outputs/holdings.parquet")
    plan = pd.read_parquet(p4_root / "outputs/backtest_vintage_plan.parquet")
    plan_fields = plan[
        [
            "stable_row_id",
            "target_exit_timestamp",
        ]
    ].drop_duplicates("stable_row_id")
    holdings = holdings.merge(
        plan_fields, on="stable_row_id", how="left", validate="one_to_one"
    )
    required = pd.DataFrame(
        {
            "requirement_id": holdings["stable_row_id"].map(
                lambda value: f"holding:{value}"
            ),
            "instrument_role": "holding",
            "stable_row_id": holdings["stable_row_id"],
            "sec_cik": holdings["cik"].astype(str).str.zfill(10),
            "ticker": holdings["ticker"].astype(str).str.upper(),
            "required_start": pd.to_datetime(
                holdings["entry_timestamp"], utc=True
            ),
            "required_end": pd.to_datetime(
                holdings["target_exit_timestamp"], utc=True
            ),
        }
    )
    exchange_document = _read_json_gzip(exchange_index_path)
    by_ticker: dict[str, list[str]] = {}
    for row in exchange_document.get("data", []):
        by_ticker.setdefault(str(row[2]).upper(), []).append(
            str(row[0]).zfill(10)
        )
    benchmark_rows = []
    required_start = required["required_start"].min()
    required_end = required["required_end"].max()
    for ticker in BENCHMARK_SYMBOLS:
        ciks = sorted(set(by_ticker.get(ticker, [])))
        benchmark_rows.append(
            {
                "requirement_id": f"benchmark:{ticker}",
                "instrument_role": "benchmark",
                "stable_row_id": pd.NA,
                "sec_cik": ciks[0] if len(ciks) == 1 else "",
                "ticker": ticker,
                "required_start": required_start,
                "required_end": required_end,
            }
        )
    return pd.concat(
        [required, pd.DataFrame(benchmark_rows)],
        ignore_index=True,
    )


def reconcile_coverage(
    required: pd.DataFrame,
    normalized: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    listings = normalized["listings"]
    securities = normalized["securities"]
    raw = normalized["raw_evidence"]
    raw_by_source = raw.groupby("source_id")["source_id"].first().to_dict()
    rows = []
    for item in required.itertuples(index=False):
        issuer_id = (
            f"issuer:us-sec:{str(item.sec_cik).zfill(10)}"
            if str(item.sec_cik).strip()
            else pd.NA
        )
        candidates = listings[
            listings["ticker"].eq(str(item.ticker).upper())
        ]
        if pd.notna(issuer_id):
            security_ids = securities.loc[
                securities["issuer_id"].eq(issuer_id), "security_id"
            ]
            candidates = candidates[
                candidates["security_id"].isin(security_ids)
            ]
        reasons: list[str] = []
        if candidates.empty:
            status = "unsupported"
            identity_status = "unsupported"
            listing_status = "unsupported"
            security_type_status = "unsupported"
            event_status = "unsupported"
            source_ids: list[str] = []
            security_id = pd.NA
            reasons.append("identity_or_listing_evidence_unavailable")
        elif len(candidates) > 1:
            status = "conflicting"
            identity_status = "conflicting"
            listing_status = "conflicting"
            security_type_status = "unsupported"
            event_status = "unresolved"
            source_ids = sorted(set(candidates["source_id"]))
            security_id = pd.NA
            reasons.append("multiple_security_listing_candidates")
        else:
            candidate = candidates.iloc[0]
            security_id = candidate["security_id"]
            source_ids = [candidate["source_id"]]
            security = securities[
                securities["security_id"].eq(security_id)
            ].iloc[0]
            if candidate["confidence"] == "conflicting":
                status = "conflicting"
                identity_status = "conflicting"
                listing_status = "conflicting"
                reasons.append("conflicting_current_sec_association")
            else:
                status = "ambiguous"
                identity_status = "matched_current_only"
                listing_status = "ambiguous_effective_dates"
                reasons.append("historical_listing_effective_dates_unavailable")
            security_type_status = str(security["security_type_status"])
            if security_type_status != "supported":
                reasons.append("security_type_history_unavailable")
            event_status = "unresolved"
            reasons.append("complete_corporate_action_terms_unavailable")
            retrieved = pd.to_datetime(
                candidate["source_retrieved_at"], utc=True, errors="coerce"
            )
            if pd.isna(retrieved) or item.required_end > retrieved:
                status = "unsupported"
                reasons.append("required_interval_extends_beyond_retrieval")
        source_ids = [
            source for source in source_ids if source in raw_by_source
        ]
        rows.append(
            {
                "requirement_id": item.requirement_id,
                "instrument_role": item.instrument_role,
                "stable_row_id": item.stable_row_id,
                "issuer_id": issuer_id,
                "security_id": security_id,
                "ticker": item.ticker,
                "required_start": item.required_start,
                "required_end": item.required_end,
                "coverage_status": status,
                "identity_status": identity_status,
                "listing_status": listing_status,
                "security_type_status": security_type_status,
                "event_status": event_status,
                "price_adjustment_status": "unsupported",
                "reason_codes": json.dumps(sorted(set(reasons))),
                "source_ids": json.dumps(source_ids),
            }
        )
    return pd.DataFrame(rows, columns=COVERAGE_COLUMNS)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _record(root: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def build_security_ledger(
    artifact_root: Path,
    *,
    ledger_id: str,
    d1_root: Path = DEFAULT_D1_ROOT,
    p4_root: Path = DEFAULT_P4_ROOT,
) -> Path:
    """Build one non-overwriting offline S1 ledger from frozen D1 evidence."""
    if not LEDGER_ID_PATTERN.fullmatch(ledger_id):
        raise ValueError("ledger_id must be an immutable UTC identifier")
    artifact_root = artifact_root.resolve()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"ledger target is not empty: {artifact_root}")
    artifact_root.mkdir(parents=True, exist_ok=True)

    pinned = {
        "p4_manifest": (
            p4_root / "manifest.json",
            "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7",
        ),
        "d1_review_manifest": (
            d1_root / "review/review_manifest.json",
            "ca175587494c1529d21d6e7c7567dbe3b16c55913c9b4b7a84b9b0d1d4569bb9",
        ),
        "d1_p2_candidate_manifest": (
            d1_root / "p2_review_candidate/manifest.json",
            "545c2eec17dae8cdffd81fd8e1b89ebc1ccc3b47290b7b556f485bbaa5f436d6",
        ),
    }
    validated_inputs = []
    for name, (path, expected) in pinned.items():
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(f"{name} hash mismatch")
        validated_inputs.append(
            {
                "name": name,
                "path": path.relative_to(ROOT).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": actual,
            }
        )

    source_root = d1_root / "universe"
    required = required_instruments(
        p4_root,
        source_root / "raw/index/company_tickers_exchange.json.gz",
    )
    adapter = SecSubmissionAdapter(source_root, artifact_root)
    normalized = adapter.normalize(required)
    coverage = reconcile_coverage(required, normalized)

    output_paths: list[tuple[Path, str]] = []
    for name, frame in normalized.items():
        path = artifact_root / f"outputs/primary/{name}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        output_paths.append((path, f"primary_{name}"))
    required_path = artifact_root / "outputs/primary/required_instruments.parquet"
    required.to_parquet(required_path, index=False)
    output_paths.append((required_path, "required_p4_instruments"))
    coverage_path = artifact_root / "outputs/primary/coverage.parquet"
    coverage.to_parquet(coverage_path, index=False)
    output_paths.append((coverage_path, "coverage_reconciliation"))

    price_contract_path = (
        artifact_root / "contracts/price_adjustment_semantics.json"
    )
    _write_json(price_contract_path, PRICE_ADJUSTMENT_CONTRACT)
    output_paths.append((price_contract_path, "price_adjustment_contract"))
    event_contract_path = artifact_root / "contracts/event_semantics.json"
    _write_json(
        event_contract_path,
        {
            "schema_version": 1,
            "supported_event_types": [
                "ticker_change", "exchange_change", "merger", "bankruptcy",
                "suspension", "delisting", "security_type_change",
            ],
            "time_axes": {
                "effective_at": "when the event changes security state",
                "source_published_at": "when the source made evidence public",
                "source_retrieved_at": "when exact response bytes were obtained",
            },
            "unsupported_policy": (
                "Do not create a supported event from disappearance, name "
                "similarity, form family alone, or model knowledge."
            ),
            "submission_index_policy": (
                "Forms 25/25-NSE and registration-termination forms are "
                "unresolved filing indicators until primary terms establish "
                "effective time, security scope, and outcome."
            ),
        },
    )
    output_paths.append((event_contract_path, "event_semantics_contract"))
    schema_path = artifact_root / "contracts/schemas.json"
    _write_json(
        schema_path,
        {
            "schema_version": 1,
            "issuer": list(ISSUER_COLUMNS),
            "security": list(SECURITY_COLUMNS),
            "listing": list(LISTING_COLUMNS),
            "event": list(EVENT_COLUMNS),
            "coverage": list(COVERAGE_COLUMNS),
            "raw_evidence": list(RAW_EVIDENCE_COLUMNS),
        },
    )
    output_paths.append((schema_path, "stable_schema_contract"))

    sensitivity_path = (
        artifact_root / "sensitivity/legacy_minus_50_policy.json"
    )
    _write_json(sensitivity_path, SENSITIVITY_CONTRACT)
    output_paths.append((sensitivity_path, "separate_policy_sensitivity"))

    coverage_counts = {
        status: int(coverage["coverage_status"].eq(status).sum())
        for status in ("matched", "ambiguous", "unsupported", "conflicting")
    }
    summary_path = artifact_root / "support/coverage_summary.json"
    _write_json(
        summary_path,
        {
            "schema_version": 1,
            "required_instruments": len(coverage),
            "holding_rows": int(
                coverage["instrument_role"].eq("holding").sum()
            ),
            "benchmark_instruments": int(
                coverage["instrument_role"].eq("benchmark").sum()
            ),
            "coverage_counts": coverage_counts,
            "primary_results_available": coverage_counts["unsupported"] == 0
            and coverage_counts["conflicting"] == 0
            and coverage_counts["ambiguous"] == 0,
            "official_performance_calculated": False,
            "official_performance_claimed": False,
            "unsupported_exit_returns_available": False,
            "delisting_returns_available": False,
            "sensitivity_physically_separate": True,
            "sensitivity_allowed_in_observed_label_training": False,
        },
    )
    output_paths.append((summary_path, "coverage_summary"))

    raw_paths = sorted(
        path for path in (artifact_root / "raw").rglob("*") if path.is_file()
    )
    manifest = {
        "schema_version": 1,
        "artifact_class": "VERSIONED_PROVIDER_NEUTRAL_SECURITY_LEDGER",
        "ledger_id": ledger_id,
        "created_at_utc": utc_now(),
        "build_mode": "offline_from_frozen_exact_d1_responses",
        "provider_adapters": ["sec_edgar"],
        "future_paid_provider_adapter_boundary": True,
        "validated_inputs": validated_inputs,
        "records": [
            *[_record(artifact_root, path, role) for path, role in output_paths],
            *[
                _record(artifact_root, path, "preserved_exact_raw_response")
                for path in raw_paths
            ],
        ],
        "coverage": coverage_counts,
        "claim": {
            "s1_ledger_contract_implemented": True,
            "free_source_boundary_measured": True,
            "historical_effective_membership_complete": False,
            "corporate_action_terms_complete": False,
            "unsupported_primary_outcomes_remain_unavailable": True,
            "performance_calculated": False,
            "official_performance_available": False,
        },
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path
