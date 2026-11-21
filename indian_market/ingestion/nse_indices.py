from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from indian_market.database.schema import index_master, index_prices
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

ALIASES = {
    "name": {"indexname", "index", "name", "indexcode"},
    "date": {"indexdate", "date", "tradedate", "timestamp"},
    "open": {"openindexvalue", "open", "openvalue"},
    "high": {"highindexvalue", "high", "highvalue"},
    "low": {"lowindexvalue", "low", "lowvalue"},
    "close": {"closingindexvalue", "close", "closeindexvalue", "closingvalue"},
}
REQUIRED = {"name", "date", "close"}


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _date(value: str) -> date:
    for pattern in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value.strip(), pattern).date()
        except ValueError:
            pass
    raise IngestionError(f"Unsupported index date value: {value!r}")


def _number(value: str | None, field: str, line: int) -> Decimal | None:
    if value is None or value.strip() in {"", "-", "NA"}:
        return None
    try:
        number = Decimal(value.replace(",", "").strip())
    except InvalidOperation as exc:
        raise IngestionError(f"Row {line}: index {field} is not numeric") from exc
    if number <= 0:
        raise IngestionError(f"Row {line}: index {field} must be positive")
    return number


def parse_index_csv(payload: bytes) -> list[dict[str, Any]]:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IngestionError("Index CSV input is not UTF-8 encoded") from exc
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise IngestionError("Index CSV is empty or has no header")
    headers = {_key(header): header for header in reader.fieldnames}
    columns = {
        field: next((headers[a] for a in aliases if a in headers), None)
        for field, aliases in ALIASES.items()
    }
    missing = sorted(field for field in REQUIRED if columns[field] is None)
    if missing:
        raise IngestionError(
            f"Unsupported index CSV format; missing fields: {', '.join(missing)}"
        )
    results = []
    seen = set()
    seen_ids: dict[str, str] = {}
    for line, row in enumerate(reader, start=2):
        name = (row.get(columns["name"]) or "").strip()
        if not name:
            raise IngestionError(f"Row {line}: index name is blank")
        trade_date = _date(row[columns["date"]])
        values = {
            field: _number(row.get(columns[field]), field, line)
            for field in ("open", "high", "low", "close")
            if columns[field]
        }
        if values.get("close") is None:
            raise IngestionError(f"Row {line}: index close is required")
        if values.get("high") and values.get("low") and values["low"] > values["high"]:
            raise IngestionError(f"Row {line}: index low exceeds high")
        key = (name.casefold(), trade_date)
        if key in seen:
            raise IngestionError(f"Duplicate index/date record: {name} {trade_date}")
        seen.add(key)
        index_id = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        previous_name = seen_ids.get(index_id)
        if previous_name and previous_name.casefold() != name.casefold():
            raise IngestionError(
                f"Index identifier collision: {previous_name!r} and {name!r}"
            )
        seen_ids[index_id] = name
        if values.get("high") and values.get("low"):
            if values["low"] > values["high"]:
                raise IngestionError(f"Row {line}: index low exceeds high")
            for field in ("open", "close"):
                if values.get(field) and not values["low"] <= values[field] <= values["high"]:
                    raise IngestionError(f"Row {line}: index {field} is outside low/high")
        results.append(
            {
                "index_id": index_id,
                "name": name,
                "trade_date": trade_date,
                **values,
            }
        )
    if not results:
        raise IngestionError("Index CSV contains no data records")
    return results


def ingest_nse_indices(
    engine: Engine,
    source_reference: str,
    *,
    requested_date: date | None,
    raw_data_dir: Path,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    requested = requested_date.isoformat() if requested_date else None
    run_id = start_run(
        engine, source="NSE", dataset="index_prices", requested_as_of=requested
    )
    raw_file = checksum = None
    try:
        payload = fetch_payload(
            source_reference,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            client=client,
        )
        raw_file, checksum = archive_payload(
            raw_data_dir, source_name="nse_indices", payload=payload, suffix=".csv"
        )
        records = parse_index_csv(payload)
        if requested_date and any(r["trade_date"] != requested_date for r in records):
            raise IngestionError("Index report date does not match the requested date")
        seen_at = utc_now()
        counts = {
            "downloaded": 1,
            "parsed": len(records),
            "accepted": len(records),
            "inserted_or_updated": len(records),
            "rejected": 0,
            "duplicates": 0,
        }
        with engine.begin() as connection:
            index_rows = [
                {
                    "index_id": r["index_id"],
                    "exchange": "NSE",
                    "name": r["name"],
                    "source_code": r["name"],
                    "source": "NSE",
                }
                for r in records
            ]
            upsert(
                connection,
                index_master,
                index_rows,
                conflict_columns=("index_id",),
                update_columns=("exchange", "name", "source_code", "source"),
            )
            price_rows = [
                {
                    "index_id": r["index_id"],
                    "trade_date": r["trade_date"],
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r["close"],
                    "source": "NSE",
                    "source_file": raw_file,
                    "source_as_of": seen_at,
                    "ingested_at": seen_at,
                }
                for r in records
            ]
            upsert(
                connection,
                index_prices,
                price_rows,
                conflict_columns=("index_id", "trade_date"),
                update_columns=(
                    "open", "high", "low", "close", "source", "source_file",
                    "source_as_of", "ingested_at",
                ),
            )
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
                issue_type="source_or_validation_error", record_key=requested,
                details=f"{type(exc).__name__}: {exc}",
            )
        if isinstance(exc, IngestionError):
            raise
        raise IngestionError(f"NSE index ingestion failed (run {run_id})") from exc
