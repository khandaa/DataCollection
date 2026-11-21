from __future__ import annotations

import hashlib
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from sqlalchemy import insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from indian_market.database.schema import data_ingestion_log, ingestion_issues

TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


class IngestionError(RuntimeError):
    """Raised when a source cannot be ingested completely."""


class SourceNotFoundError(IngestionError):
    """Raised when a remote source has no report for the requested date."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_source_reference(source: str) -> str:
    parsed = urlsplit(source)
    if parsed.scheme not in {"http", "https"}:
        return source
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def fetch_payload(
    source: str,
    *,
    timeout_seconds: float,
    max_attempts: int,
    client: httpx.Client | None = None,
    sleep=time.sleep,
) -> bytes:
    parsed = urlsplit(source)
    if parsed.scheme in {"", "file"}:
        path = Path(parsed.path if parsed.scheme == "file" else source)
        if not path.is_file():
            raise IngestionError(f"Input file does not exist: {path}")
        return path.read_bytes()
    if parsed.scheme not in {"http", "https"}:
        raise IngestionError("Source must be a local path, file URL, HTTP, or HTTPS URL")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least one")

    owns_client = client is None
    session = client or httpx.Client(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": "dataCollection-Research/0.1"},
    )
    try:
        for attempt in range(max_attempts):
            try:
                response = session.get(source)
                if response.status_code in TRANSIENT_STATUS_CODES:
                    if attempt + 1 == max_attempts:
                        response.raise_for_status()
                    sleep(min(2**attempt, 30))
                    continue
                response.raise_for_status()
                return response.content
            except httpx.TransportError:
                if attempt + 1 == max_attempts:
                    raise
                sleep(min(2**attempt, 30))
        raise IngestionError("Source request failed after retries")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise SourceNotFoundError(
                f"Source returned HTTP 404: {safe_source_reference(source)}"
            ) from exc
        raise IngestionError(
            f"Source returned HTTP {exc.response.status_code}: "
            f"{safe_source_reference(source)}"
        ) from exc
    except httpx.TransportError as exc:
        raise IngestionError(
            f"Source request failed: {safe_source_reference(source)} "
            f"({type(exc).__name__})"
        ) from exc
    finally:
        if owns_client:
            session.close()


def archive_payload(
    raw_data_dir: Path,
    *,
    source_name: str,
    payload: bytes,
    suffix: str,
) -> tuple[str, str]:
    checksum = hashlib.sha256(payload).hexdigest()
    clean_source = re.sub(r"[^a-zA-Z0-9_-]+", "_", source_name).strip("_").lower()
    extension = suffix if suffix.startswith(".") else f".{suffix}"
    directory = raw_data_dir / clean_source
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{checksum}{extension}"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise IngestionError(f"Checksum collision detected for raw file {path}")
    else:
        with os.fdopen(descriptor, "wb") as file:
            file.write(payload)
    return str(path), checksum


def start_run(
    engine: Engine,
    *,
    source: str,
    dataset: str,
    requested_as_of: str | None,
) -> str:
    run_id = str(uuid.uuid4())
    with engine.begin() as connection:
        connection.execute(
            insert(data_ingestion_log).values(
                run_id=run_id,
                source=source,
                dataset=dataset,
                requested_as_of=requested_as_of,
                started_at=utc_now(),
                status="started",
                counts={},
            )
        )
    return run_id


def finish_run(
    connection: Connection,
    run_id: str,
    *,
    status: str,
    counts: dict[str, int],
    raw_file: str | None,
    checksum: str | None,
    error_summary: str | None = None,
) -> None:
    connection.execute(
        update(data_ingestion_log)
        .where(data_ingestion_log.c.run_id == run_id)
        .values(
            finished_at=utc_now(),
            status=status,
            counts=counts,
            raw_file=raw_file,
            checksum_sha256=checksum,
            error_summary=error_summary,
        )
    )


def record_issue(
    connection: Connection,
    *,
    run_id: str,
    row_number: int | None,
    issue_type: str,
    record_key: str | None,
    details: str,
) -> None:
    connection.execute(
        insert(ingestion_issues).values(
            run_id=run_id,
            row_number=row_number,
            issue_type=issue_type,
            record_key=record_key,
            details=details[:4000],
        )
    )


def upsert(
    connection: Connection,
    table: Any,
    values: list[dict[str, Any]],
    *,
    conflict_columns: tuple[str, ...],
    update_columns: tuple[str, ...],
) -> None:
    if not values:
        return
    dialect = connection.dialect.name
    if dialect == "mysql":
        statement = mysql_insert(table).values(values)
        statement = statement.on_duplicate_key_update(
            **{
                column: statement.inserted[column]
                for column in update_columns
            }
        )
        connection.execute(statement)
    elif dialect == "sqlite":
        statement = sqlite_insert(table).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=[table.c[column] for column in conflict_columns],
            set_={
                column: getattr(statement.excluded, column)
                for column in update_columns
            },
        )
        connection.execute(statement)
    else:
        for row in values:
            conditions = [
                table.c[column] == row[column] for column in conflict_columns
            ]
            existing = connection.execute(
                select(table).where(*conditions)
            ).first()
            if existing:
                connection.execute(
                    update(table).where(*conditions).values(
                        **{column: row[column] for column in update_columns}
                    )
                )
            else:
                connection.execute(insert(table).values(**row))
        return
