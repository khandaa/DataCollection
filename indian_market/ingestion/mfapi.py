from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from sqlalchemy.engine import Engine

from indian_market.database.schema import mutual_fund_nav, mutual_fund_schemes
from indian_market.ingestion.common import (
    IngestionError,
    archive_payload,
    fetch_payload,
    finish_run,
    record_issue,
    start_run,
    upsert,
    utc_now,
)


def _nav_date(value: str) -> date:
    for pattern in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise IngestionError(f"Unsupported MFAPI NAV date: {value!r}")


def _scheme_values(meta: dict[str, Any], retrieved_at: datetime) -> dict[str, Any]:
    try:
        code = int(meta.get("scheme_code", meta.get("schemeCode")))
    except (TypeError, ValueError) as exc:
        raise IngestionError("MFAPI response has no valid scheme code") from exc
    name = meta.get("scheme_name", meta.get("schemeName"))
    if not name:
        raise IngestionError(f"MFAPI response for scheme {code} has no scheme name")
    return {
        "scheme_code": code,
        "scheme_name": str(name).strip(),
        "fund_house": meta.get("fund_house", meta.get("fundHouse")),
        "scheme_type": meta.get("scheme_type", meta.get("schemeType")),
        "scheme_category": meta.get("scheme_category", meta.get("schemeCategory")),
        "isin_growth": meta.get("isin_growth", meta.get("isinGrowth")),
        "isin_div_reinvestment": meta.get(
            "isin_div_reinvestment", meta.get("isinDivReinvestment")
        ),
        "source": "MFAPI",
        "retrieved_at": retrieved_at,
    }


def parse_mfapi_history(payload: bytes, requested_scheme_code: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        body = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestionError("MFAPI response is not valid JSON") from exc
    if not isinstance(body, dict) or not isinstance(body.get("meta"), dict):
        raise IngestionError("MFAPI history response has no metadata object")
    metadata = _scheme_values(body["meta"], utc_now())
    if metadata["scheme_code"] != requested_scheme_code:
        raise IngestionError(
            f"MFAPI returned scheme {metadata['scheme_code']} for requested "
            f"scheme {requested_scheme_code}"
        )
    data = body.get("data")
    if not isinstance(data, list) or not data:
        raise IngestionError(f"MFAPI returned no NAV history for scheme {requested_scheme_code}")
    rows = []
    dates = set()
    for position, item in enumerate(data, start=1):
        if not isinstance(item, dict) or "date" not in item or "nav" not in item:
            raise IngestionError(f"MFAPI NAV record {position} is malformed")
        nav_date = _nav_date(str(item["date"]))
        try:
            nav = Decimal(str(item["nav"]).replace(",", "").strip())
        except InvalidOperation as exc:
            raise IngestionError(f"MFAPI NAV record {position} has invalid NAV") from exc
        if nav <= 0:
            raise IngestionError(f"MFAPI NAV record {position} must be positive")
        if nav_date in dates:
            raise IngestionError(f"MFAPI response contains duplicate NAV date {nav_date}")
        dates.add(nav_date)
        rows.append({"scheme_code": requested_scheme_code, "nav_date": nav_date, "nav": nav})
    return metadata, rows


def parse_mfapi_search(payload: bytes) -> list[dict[str, Any]]:
    try:
        body = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestionError("MFAPI search response is not valid JSON") from exc
    if not isinstance(body, list):
        raise IngestionError("MFAPI search response must be a list")
    found = []
    for position, item in enumerate(body, start=1):
        if not isinstance(item, dict):
            raise IngestionError(f"MFAPI search result {position} is malformed")
        found.append(_scheme_values(item, utc_now()))
    return found


def ingest_mfapi_scheme(
    engine: Engine,
    scheme_code: int,
    *,
    base_url: str,
    raw_data_dir: Path,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    if scheme_code <= 0:
        raise ValueError("scheme_code must be a positive integer")
    endpoint = f"{base_url.rstrip('/')}/mf/{scheme_code}"
    run_id = start_run(
        engine, source="MFAPI", dataset="mutual_fund_nav",
        requested_as_of=str(scheme_code),
    )
    raw_file = checksum = None
    try:
        payload = fetch_payload(
            endpoint,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            client=client,
        )
        raw_file, checksum = archive_payload(
            raw_data_dir, source_name="mfapi", payload=payload, suffix=".json"
        )
        metadata, records = parse_mfapi_history(payload, scheme_code)
        retrieved_at = utc_now()
        metadata["retrieved_at"] = retrieved_at
        counts = {
            "downloaded": 1,
            "parsed": len(records),
            "accepted": len(records),
            "inserted_or_updated": len(records),
            "rejected": 0,
            "duplicates": 0,
        }
        nav_rows = [
            {
                **record,
                "source": "MFAPI",
                "source_file": raw_file,
                "source_as_of": retrieved_at,
                "ingested_at": retrieved_at,
            }
            for record in records
        ]
        with engine.begin() as connection:
            upsert(
                connection,
                mutual_fund_schemes,
                [metadata],
                conflict_columns=("scheme_code",),
                update_columns=(
                    "scheme_name", "fund_house", "scheme_type", "scheme_category",
                    "isin_growth", "isin_div_reinvestment", "source", "retrieved_at",
                ),
            )
            upsert(
                connection,
                mutual_fund_nav,
                nav_rows,
                conflict_columns=("scheme_code", "nav_date"),
                update_columns=(
                    "nav", "source", "source_file", "source_as_of", "ingested_at",
                ),
            )
            finish_run(
                connection, run_id, status="succeeded", counts=counts,
                raw_file=raw_file, checksum=checksum,
            )
        return {
            "run_id": run_id,
            "scheme_code": scheme_code,
            "scheme_name": metadata["scheme_name"],
            "latest_nav_date": max(row["nav_date"] for row in records).isoformat(),
            **counts,
            "raw_file": raw_file,
        }
    except Exception as exc:
        with engine.begin() as connection:
            finish_run(
                connection, run_id, status="failed", counts={"rejected": 1},
                raw_file=raw_file, checksum=checksum,
                error_summary=f"{type(exc).__name__}: {exc}"[:4000],
            )
            record_issue(
                connection, run_id=run_id, row_number=None,
                issue_type="source_or_validation_error",
                record_key=str(scheme_code),
                details=f"{type(exc).__name__}: {exc}",
            )
        if isinstance(exc, IngestionError):
            raise
        raise IngestionError(f"MFAPI ingestion failed (run {run_id})") from exc


def search_mfapi_schemes(
    engine: Engine,
    query: str,
    *,
    base_url: str,
    raw_data_dir: Path,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    if not query.strip():
        raise ValueError("search query must not be blank")
    endpoint = f"{base_url.rstrip('/')}/mf/search?q={quote(query, safe='')}"
    run_id = start_run(
        engine, source="MFAPI", dataset="mutual_fund_schemes",
        requested_as_of=query[:64],
    )
    raw_file = checksum = None
    try:
        payload = fetch_payload(
            endpoint,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            client=client,
        )
        raw_file, checksum = archive_payload(
            raw_data_dir, source_name="mfapi", payload=payload, suffix=".json"
        )
        schemes = parse_mfapi_search(payload)
        with engine.begin() as connection:
            upsert(
                connection,
                mutual_fund_schemes,
                schemes,
                conflict_columns=("scheme_code",),
                update_columns=(
                    "scheme_name", "fund_house", "scheme_type", "scheme_category",
                    "isin_growth", "isin_div_reinvestment", "source", "retrieved_at",
                ),
            )
            counts = {
                "downloaded": 1,
                "parsed": len(schemes),
                "accepted": len(schemes),
                "inserted_or_updated": len(schemes),
                "rejected": 0,
            }
            finish_run(
                connection, run_id, status="succeeded", counts=counts,
                raw_file=raw_file, checksum=checksum,
            )
        return {"run_id": run_id, **counts, "raw_file": raw_file}
    except Exception as exc:
        with engine.begin() as connection:
            finish_run(
                connection, run_id, status="failed", counts={"rejected": 1},
                raw_file=raw_file, checksum=checksum,
                error_summary=f"{type(exc).__name__}: {exc}"[:4000],
            )
            record_issue(
                connection, run_id=run_id, row_number=None,
                issue_type="source_or_validation_error", record_key=query[:256],
                details=f"{type(exc).__name__}: {exc}",
            )
        if isinstance(exc, IngestionError):
            raise
        raise IngestionError(f"MFAPI search failed (run {run_id})") from exc
