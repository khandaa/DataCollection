from __future__ import annotations

import csv
import io
import re
import uuid
import zipfile
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import insert, select, update
from sqlalchemy.engine import Engine

from indian_market.database.schema import daily_prices, security_master
from indian_market.ingestion.common import (
    IngestionError,
    SourceNotFoundError,
    archive_payload,
    fetch_payload,
    finish_run,
    record_issue,
    start_run,
    upsert,
    utc_now,
)

FIELD_ALIASES = {
    "trade_date": {"traddt", "tradet", "tradedate", "date", "timestamp"},
    "symbol": {"tckrsymb", "symbol", "tradingsymbol"},
    "series": {"sctysrs", "series"},
    "isin": {"isin", "isinno"},
    "name": {"fininstrmactynm", "companyname", "name"},
    "open": {"opnpric", "open", "openprice"},
    "high": {"hghpric", "high", "highprice"},
    "low": {"lwpric", "low", "lowprice"},
    "close": {"clspric", "close", "closeprice", "last"},
    "volume": {"ttltradgvol", "tottrdqty", "volume", "quantitytraded"},
    "traded_value": {"ttltrfval", "tottrdval", "tradedvalue", "turnover"},
}
REQUIRED_FIELDS = {"symbol", "series", "open", "high", "low", "close"}


def _header_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _decode_csv_payload(payload: bytes) -> str:
    if payload[:2] == b"PK":
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                members = [
                    name for name in archive.namelist()
                    if name.lower().endswith(".csv") and not name.startswith("__MACOSX/")
                ]
                if len(members) != 1:
                    raise IngestionError(
                        f"Expected one CSV in archive; found {len(members)}"
                    )
                return archive.read(members[0]).decode("utf-8-sig")
        except zipfile.BadZipFile as exc:
            raise IngestionError("Downloaded input is not a valid ZIP archive") from exc
    try:
        return payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IngestionError("CSV input is not UTF-8 encoded") from exc


def _parse_trade_date(value: str | None, requested_date: date | None) -> date:
    if value:
        cleaned = value.strip()
        for pattern in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
            try:
                return datetime.strptime(cleaned, pattern).date()
            except ValueError:
                continue
        raise IngestionError(f"Unsupported trade date value: {value!r}")
    if requested_date is None:
        raise IngestionError("Trade date is missing; provide --date to supply the expected date")
    return requested_date


def _decimal(value: str | None, field: str, row_number: int) -> Decimal | None:
    if value is None or value.strip() in {"", "-", "NA", "N/A"}:
        return None
    try:
        return Decimal(value.replace(",", "").strip())
    except InvalidOperation as exc:
        raise IngestionError(
            f"Row {row_number}: {field} is not numeric: {value!r}"
        ) from exc


def parse_nse_eod(payload: bytes, requested_date: date | None = None) -> list[dict[str, Any]]:
    if not payload:
        raise SourceNotFoundError("NSE EOD report contains no data")
    text = _decode_csv_payload(payload)
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise IngestionError("CSV is empty or has no header")
    source_headers = {_header_key(header): header for header in reader.fieldnames}
    columns = {
        field: next(
            (source_headers[alias] for alias in aliases if alias in source_headers),
            None,
        )
        for field, aliases in FIELD_ALIASES.items()
    }
    missing = sorted(field for field in REQUIRED_FIELDS if columns[field] is None)
    if missing:
        raise IngestionError(
            f"Unsupported NSE EOD format; missing required fields: {', '.join(missing)}"
        )

    records: list[dict[str, Any]] = []
    for row_number, row in enumerate(reader, start=2):
        symbol = (row.get(columns["symbol"]) or "").strip().upper()
        series = (row.get(columns["series"]) or "").strip().upper()
        if not symbol or not series:
            raise IngestionError(f"Row {row_number}: symbol or series is blank")
        trade_date = _parse_trade_date(
            row.get(columns["trade_date"]) if columns["trade_date"] else None,
            requested_date,
        )
        if requested_date and trade_date != requested_date:
            raise IngestionError(
                f"Row {row_number}: source trade date {trade_date} "
                f"does not match requested date {requested_date}"
            )
        values = {
            field: _decimal(row.get(columns[field]), field, row_number)
            for field in ("open", "high", "low", "close", "volume", "traded_value")
            if columns[field]
        }
        for field in ("open", "high", "low", "close"):
            if values[field] is None or values[field] <= 0:
                raise IngestionError(
                    f"Row {row_number}: {field} must be a positive number"
                )
        if values["low"] > values["high"]:
            raise IngestionError(f"Row {row_number}: low exceeds high")
        if not values["low"] <= values["open"] <= values["high"]:
            raise IngestionError(f"Row {row_number}: open is outside low/high")
        if not values["low"] <= values["close"] <= values["high"]:
            raise IngestionError(f"Row {row_number}: close is outside low/high")
        for field in ("volume", "traded_value"):
            if values.get(field) is not None and values[field] < 0:
                raise IngestionError(f"Row {row_number}: {field} cannot be negative")
        isin = (row.get(columns["isin"]) or "").strip().upper() if columns["isin"] else ""
        records.append(
            {
                "row_number": row_number,
                "trade_date": trade_date,
                "symbol": symbol,
                "series": series,
                "isin": isin or None,
                "name": (
                    (row.get(columns["name"]) or "").strip()
                    if columns["name"] else None
                ),
                **values,
            }
        )
    if not records:
        raise SourceNotFoundError("NSE EOD report contains no data records")

    keys: set[tuple[str, str, date]] = set()
    for record in records:
        key = (record["symbol"], record["series"], record["trade_date"])
        if key in keys:
            raise IngestionError(
                f"Duplicate symbol/series/date in report: "
                f"{key[0]}/{key[1]}/{key[2]}"
            )
        keys.add(key)
    return records


def _resolve_security(
    connection,
    record: dict[str, Any],
    *,
    source: str,
    seen_at: datetime,
) -> str:
    conditions = [
        security_master.c.exchange == "NSE",
        security_master.c.symbol == record["symbol"],
        security_master.c.series == record["series"],
        security_master.c.effective_from <= record["trade_date"],
    ]
    conditions.append(
        (security_master.c.effective_to.is_(None))
        | (security_master.c.effective_to >= record["trade_date"])
    )
    matches = connection.execute(
        select(
            security_master.c.security_id,
            security_master.c.isin,
            security_master.c.effective_from,
        ).where(*conditions)
    ).all()
    if len(matches) > 1:
        raise IngestionError(
            f"Ambiguous security mapping for {record['symbol']}/{record['series']}"
        )
    if matches:
        mapping = matches[0]
        security_id = mapping.security_id
        if mapping.isin and record["isin"] and mapping.isin != record["isin"]:
            if mapping.effective_from >= record["trade_date"]:
                raise IngestionError(
                    f"ISIN conflict for {record['symbol']}/{record['series']} "
                    f"on mapping effective date {mapping.effective_from}"
                )
            connection.execute(
                update(security_master)
                .where(security_master.c.security_id == security_id)
                .values(
                    effective_to=date.fromordinal(record["trade_date"].toordinal() - 1),
                    active=False,
                )
            )
            matches = []
        else:
            connection.execute(
                update(security_master)
                .where(security_master.c.security_id == security_id)
                .values(
                    isin=record["isin"] or security_master.c.isin,
                    name=record["name"] or security_master.c.name,
                    active=True,
                    source=source,
                    last_seen_at=seen_at,
                )
            )
            return security_id

    if record["isin"]:
        prior_mapping = connection.execute(
            select(security_master.c.security_id).where(
                security_master.c.exchange == "NSE",
                security_master.c.isin == record["isin"],
                security_master.c.effective_to.is_(None),
                security_master.c.effective_from < record["trade_date"],
            )
        ).scalars().all()
        for prior_id in prior_mapping:
            connection.execute(
                update(security_master)
                .where(security_master.c.security_id == prior_id)
                .values(
                    effective_to=date.fromordinal(record["trade_date"].toordinal() - 1),
                    active=False,
                )
            )

    security_id = str(uuid.uuid4())
    connection.execute(
        insert(security_master).values(
            security_id=security_id,
            exchange="NSE",
            isin=record["isin"],
            symbol=record["symbol"],
            series=record["series"],
            name=record["name"],
            effective_from=record["trade_date"],
            effective_to=None,
            active=True,
            source=source,
            last_seen_at=seen_at,
        )
    )
    return security_id


def ingest_nse_eod(
    engine: Engine,
    source_reference: str,
    *,
    requested_date: date | None,
    raw_data_dir: Path,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    price_move_alert_percent: float = 0.3,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    if price_move_alert_percent <= 0:
        raise ValueError("price_move_alert_percent must be positive")
    requested = requested_date.isoformat() if requested_date else None
    run_id = start_run(
        engine, source="NSE", dataset="daily_prices", requested_as_of=requested
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
            raw_data_dir, source_name="nse_eod", payload=payload, suffix=".bin"
        )
        records = parse_nse_eod(payload, requested_date=requested_date)
        counts = {
            "downloaded": 1,
            "parsed": len(records),
            "accepted": 0,
            "inserted_or_updated": 0,
            "rejected": 0,
            "duplicates": 0,
            "unmapped": 0,
            "missing_expected": 0,
            "price_move_alerts": 0,
        }
        seen_at = utc_now()
        with engine.begin() as connection:
            expected = connection.execute(
                select(security_master.c.symbol, security_master.c.series).where(
                    security_master.c.exchange == "NSE",
                    security_master.c.active.is_(True),
                    security_master.c.effective_from <= records[0]["trade_date"],
                    (security_master.c.effective_to.is_(None))
                    | (security_master.c.effective_to >= records[0]["trade_date"]),
                )
            ).all()
            incoming = {(r["symbol"], r["series"]) for r in records}
            missing = sorted(set(expected) - incoming)
            counts["missing_expected"] = len(missing)
            for symbol, series in missing:
                record_issue(
                    connection,
                    run_id=run_id,
                    row_number=None,
                    issue_type="missing_expected_security",
                    record_key=f"{symbol}/{series}",
                    details=(
                        "Security is in the active reference universe for "
                        f"{records[0]['trade_date']} but absent from the EOD report"
                    ),
                )
            price_rows = []
            for record in records:
                security_id = _resolve_security(
                    connection, record, source="NSE", seen_at=seen_at
                )
                previous_close = connection.execute(
                    select(daily_prices.c.close)
                    .where(
                        daily_prices.c.security_id == security_id,
                        daily_prices.c.trade_date < record["trade_date"],
                    )
                    .order_by(daily_prices.c.trade_date.desc())
                    .limit(1)
                ).scalar_one_or_none()
                if previous_close is not None:
                    relative_move = abs(record["close"] - previous_close) / previous_close
                    if relative_move >= Decimal(str(price_move_alert_percent)):
                        counts["price_move_alerts"] += 1
                        record_issue(
                            connection,
                            run_id=run_id,
                            row_number=record["row_number"],
                            issue_type="large_price_move",
                            record_key=f"{record['symbol']}/{record['series']}",
                            details=(
                                f"Close changed by {relative_move:.2%} from "
                                f"{previous_close} to {record['close']}"
                            ),
                        )
                price_rows.append(
                    {
                        "security_id": security_id,
                        "trade_date": record["trade_date"],
                        "open": record["open"],
                        "high": record["high"],
                        "low": record["low"],
                        "close": record["close"],
                        "volume": record.get("volume"),
                        "traded_value": record.get("traded_value"),
                        "source": "NSE",
                        "source_file": raw_file,
                        "source_as_of": seen_at,
                        "ingested_at": seen_at,
                    }
                )
                counts["accepted"] += 1
            upsert(
                connection,
                daily_prices,
                price_rows,
                conflict_columns=("security_id", "trade_date"),
                update_columns=(
                    "open", "high", "low", "close", "volume", "traded_value",
                    "source", "source_file", "source_as_of", "ingested_at",
                ),
            )
            counts["inserted_or_updated"] = len(price_rows)
            finish_run(
                connection,
                run_id,
                status="partial" if missing else "succeeded",
                counts=counts,
                raw_file=raw_file,
                checksum=checksum,
            )
        return {
            "run_id": run_id,
            "status": "partial" if counts["missing_expected"] else "succeeded",
            **counts,
            "raw_file": raw_file,
        }
    except Exception as exc:
        with engine.begin() as connection:
            finish_run(
                connection,
                run_id,
                status="failed",
                counts={"rejected": 1},
                raw_file=raw_file,
                checksum=checksum,
                error_summary=f"{type(exc).__name__}: {exc}"[:4000],
            )
            record_issue(
                connection,
                run_id=run_id,
                row_number=None,
                issue_type="source_or_validation_error",
                record_key=requested,
                details=f"{type(exc).__name__}: {exc}",
            )
        if isinstance(exc, IngestionError):
            raise
        raise IngestionError(f"NSE EOD ingestion failed (run {run_id})") from exc
