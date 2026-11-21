from __future__ import annotations

import csv
import io
import re
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select, update
from sqlalchemy.engine import Engine

from indian_market.database.schema import security_master
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
    "symbol": {"tckrsymb", "symbol", "tradingsymbol"},
    "series": {"sctysrs", "series"},
    "isin": {"isin", "isinno", "isincode"},
    "name": {"fininstrmactynm", "fininstrmnm", "companyname", "nameofcompany", "name"},
}


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _close_mapping(connection, security_id: str, effective_date: date) -> None:
    connection.execute(
        update(security_master)
        .where(security_master.c.security_id == security_id)
        .values(
            effective_to=date.fromordinal(effective_date.toordinal() - 1),
            active=False,
        )
    )


def parse_nse_security_master(payload: bytes, effective_from: date) -> list[dict[str, Any]]:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IngestionError("NSE security master is not UTF-8 encoded") from exc
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise IngestionError("Security master CSV is empty or has no header")
    headers = {_key(header): header for header in reader.fieldnames}
    columns = {
        field: next((headers[a] for a in aliases if a in headers), None)
        for field, aliases in ALIASES.items()
    }
    missing = [field for field in ("symbol", "series") if columns[field] is None]
    if missing:
        raise IngestionError(
            f"Unsupported security master format; missing: {', '.join(missing)}"
        )
    records = []
    seen = set()
    for line, row in enumerate(reader, start=2):
        symbol = (row.get(columns["symbol"]) or "").strip().upper()
        series = (row.get(columns["series"]) or "").strip().upper()
        if not symbol or not series:
            raise IngestionError(f"Security master row {line}: symbol/series is blank")
        key = (symbol, series)
        if key in seen:
            raise IngestionError(f"Duplicate security master mapping: {symbol}/{series}")
        seen.add(key)
        isin = (row.get(columns["isin"]) or "").strip().upper() if columns["isin"] else ""
        name = (row.get(columns["name"]) or "").strip() if columns["name"] else ""
        records.append(
            {
                "security_id": str(uuid.uuid4()),
                "exchange": "NSE",
                "isin": isin or None,
                "symbol": symbol,
                "series": series,
                "name": name or None,
                "effective_from": effective_from,
                "effective_to": None,
                "active": True,
                "source": "NSE",
                "last_seen_at": utc_now(),
            }
        )
    if not records:
        raise IngestionError("Security master contains no instrument records")
    return records


def ingest_nse_security_master(
    engine: Engine,
    source_reference: str,
    *,
    effective_from: date,
    raw_data_dir: Path,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    run_id = start_run(
        engine, source="NSE", dataset="security_master",
        requested_as_of=effective_from.isoformat(),
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
            raw_data_dir, source_name="nse_security_master",
            payload=payload, suffix=".csv",
        )
        records = parse_nse_security_master(payload, effective_from)
        with engine.begin() as connection:
            incoming_keys = {(record["symbol"], record["series"]) for record in records}
            active_mappings = connection.execute(
                select(
                    security_master.c.security_id,
                    security_master.c.symbol,
                    security_master.c.series,
                    security_master.c.isin,
                    security_master.c.name,
                    security_master.c.effective_from,
                ).where(
                    security_master.c.exchange == "NSE",
                    security_master.c.active.is_(True),
                    security_master.c.effective_to.is_(None),
                    security_master.c.effective_from <= effective_from,
                )
            ).all()
            active_by_key = {
                (mapping.symbol, mapping.series): mapping for mapping in active_mappings
            }
            closed_ids: set[str] = set()
            new_records = []
            for record in records:
                key = (record["symbol"], record["series"])
                same_key = active_by_key.get(key)
                if same_key and (
                    not same_key.isin or not record["isin"]
                    or same_key.isin == record["isin"]
                ):
                    connection.execute(
                        update(security_master)
                        .where(security_master.c.security_id == same_key.security_id)
                        .values(
                            isin=record["isin"] or same_key.isin,
                            name=record["name"] or same_key.name,
                            active=True,
                            source="NSE",
                            last_seen_at=record["last_seen_at"],
                        )
                    )
                    continue
                if same_key:
                    if same_key.effective_from >= effective_from:
                        raise IngestionError(
                            f"Cannot represent same-day identity change for "
                            f"{record['symbol']}/{record['series']}"
                        )
                    _close_mapping(connection, same_key.security_id, effective_from)
                    closed_ids.add(same_key.security_id)

                if record["isin"]:
                    for prior in active_mappings:
                        if (
                            prior.security_id not in closed_ids
                            and prior.isin == record["isin"]
                            and (prior.symbol, prior.series) != key
                        ):
                            if prior.effective_from >= effective_from:
                                raise IngestionError(
                                    f"Cannot represent same-day symbol change for "
                                    f"ISIN {record['isin']}"
                                )
                            _close_mapping(connection, prior.security_id, effective_from)
                            closed_ids.add(prior.security_id)
                new_records.append(record)

            for prior in active_mappings:
                key = (prior.symbol, prior.series)
                if key not in incoming_keys and prior.security_id not in closed_ids:
                    if prior.effective_from >= effective_from:
                        continue
                    _close_mapping(connection, prior.security_id, effective_from)
                    closed_ids.add(prior.security_id)
            upsert(
                connection, security_master, new_records,
                conflict_columns=("exchange", "symbol", "series", "effective_from"),
                update_columns=(
                    "isin", "name", "effective_to", "active", "source", "last_seen_at",
                ),
            )
            counts = {
                "downloaded": 1,
                "parsed": len(records),
                "accepted": len(records),
                "inserted_or_updated": len(records),
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
                issue_type="source_or_validation_error",
                record_key=effective_from.isoformat(),
                details=f"{type(exc).__name__}: {exc}",
            )
        if isinstance(exc, IngestionError):
            raise
        raise IngestionError(f"NSE security master ingestion failed (run {run_id})") from exc
