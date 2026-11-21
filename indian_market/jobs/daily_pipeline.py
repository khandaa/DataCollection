from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from indian_market.config import Settings
from indian_market.database.connection import create_database_engine
from indian_market.ingestion.common import IngestionError
from indian_market.ingestion.mfapi import ingest_mfapi_scheme, search_mfapi_schemes
from indian_market.ingestion.nse_eod import ingest_nse_eod
from indian_market.ingestion.nse_indices import ingest_nse_indices
from indian_market.ingestion.nse_master import ingest_nse_security_master

logger = logging.getLogger("indian_market")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import Phase I market data into the local MySQL database."
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=(
            "nse-eod", "nse-index", "nse-master", "mfapi", "mfapi-search", "all",
        ),
        help="Dataset/source to ingest",
    )
    parser.add_argument("--date", type=date.fromisoformat, help="Exchange trade date (YYYY-MM-DD)")
    parser.add_argument(
        "--input",
        help="Local report path or HTTP(S) URL for an NSE EOD, index, or master file",
    )
    parser.add_argument(
        "--scheme-code",
        type=int,
        action="append",
        help="MFAPI scheme code; repeat to ingest multiple schemes",
    )
    parser.add_argument("--query", help="MFAPI scheme search text")
    return parser


def _dated_source(template: str | None, trade_date: date, label: str) -> str:
    if not template:
        raise ValueError(f"{label} source is not configured; pass --input or set its URL template")
    try:
        return template.format(date=trade_date.strftime("%Y%m%d"))
    except (KeyError, ValueError) as exc:
        raise ValueError(f"{label} URL template must use a valid {{date}} placeholder") from exc


def _print_result(result: dict[str, Any]) -> None:
    print(json.dumps(result, default=str, sort_keys=True))


def run(args: argparse.Namespace) -> int:
    load_dotenv()
    settings = Settings.from_env()
    settings.raw_data_dir.mkdir(parents=True, exist_ok=True)
    engine = create_database_engine(settings.database_url)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql("SELECT 1")
        results: list[dict[str, Any]] = []

        if args.source == "nse-eod":
            if not args.date:
                raise ValueError("--date is required for nse-eod")
            source = args.input or _dated_source(
                settings.nse_eod_url_template, args.date, "NSE EOD"
            )
            results.append(
                ingest_nse_eod(
                    engine, source, requested_date=args.date,
                    raw_data_dir=settings.raw_data_dir,
                    timeout_seconds=settings.http_timeout_seconds,
                    max_attempts=settings.http_max_attempts,
                    price_move_alert_percent=settings.price_move_alert_percent,
                )
            )
        elif args.source == "nse-index":
            if not args.date:
                raise ValueError("--date is required for nse-index")
            source = args.input or _dated_source(
                settings.nse_index_url_template, args.date, "NSE index"
            )
            results.append(
                ingest_nse_indices(
                    engine, source, requested_date=args.date,
                    raw_data_dir=settings.raw_data_dir,
                    timeout_seconds=settings.http_timeout_seconds,
                    max_attempts=settings.http_max_attempts,
                )
            )
        elif args.source == "nse-master":
            if not args.date or not args.input:
                raise ValueError("--date and --input are required for nse-master")
            results.append(
                ingest_nse_security_master(
                    engine, args.input, effective_from=args.date,
                    raw_data_dir=settings.raw_data_dir,
                    timeout_seconds=settings.http_timeout_seconds,
                    max_attempts=settings.http_max_attempts,
                )
            )
        elif args.source == "mfapi":
            if not args.scheme_code:
                raise ValueError("Provide at least one --scheme-code for mfapi")
            for scheme_code in args.scheme_code:
                results.append(
                    ingest_mfapi_scheme(
                        engine, scheme_code, base_url=settings.mfapi_base_url,
                        raw_data_dir=settings.raw_data_dir,
                        timeout_seconds=settings.http_timeout_seconds,
                        max_attempts=settings.http_max_attempts,
                    )
                )
        elif args.source == "mfapi-search":
            if not args.query:
                raise ValueError("--query is required for mfapi-search")
            results.append(
                search_mfapi_schemes(
                    engine, args.query, base_url=settings.mfapi_base_url,
                    raw_data_dir=settings.raw_data_dir,
                    timeout_seconds=settings.http_timeout_seconds,
                    max_attempts=settings.http_max_attempts,
                )
            )
        else:
            if not args.date:
                raise ValueError("--date is required for --source all")
            prices_source = _dated_source(
                settings.nse_eod_url_template, args.date, "NSE EOD"
            )
            index_source = _dated_source(
                settings.nse_index_url_template, args.date, "NSE index"
            )
            jobs = [
                lambda: ingest_nse_eod(
                    engine, prices_source, requested_date=args.date,
                    raw_data_dir=settings.raw_data_dir,
                    timeout_seconds=settings.http_timeout_seconds,
                    max_attempts=settings.http_max_attempts,
                    price_move_alert_percent=settings.price_move_alert_percent,
                ),
                lambda: ingest_nse_indices(
                    engine, index_source, requested_date=args.date,
                    raw_data_dir=settings.raw_data_dir,
                    timeout_seconds=settings.http_timeout_seconds,
                    max_attempts=settings.http_max_attempts,
                ),
            ]
            for scheme_code in args.scheme_code or []:
                jobs.append(
                    lambda code=scheme_code: ingest_mfapi_scheme(
                        engine, code, base_url=settings.mfapi_base_url,
                        raw_data_dir=settings.raw_data_dir,
                        timeout_seconds=settings.http_timeout_seconds,
                        max_attempts=settings.http_max_attempts,
                    )
                )
            for job in jobs:
                try:
                    results.append(job())
                except Exception as exc:
                    logger.exception("A required dataset failed")
                    results.append({"status": "failed", "error": str(exc)})

        for result in results:
            _print_result(result)
        statuses = {result.get("status", "succeeded") for result in results}
        if statuses == {"succeeded"}:
            return 0
        if statuses == {"partial"}:
            return 2
        return 1
    finally:
        engine.dispose()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    args = _parser().parse_args()
    try:
        exit_code = run(args)
    except (ValueError, IngestionError) as exc:
        logger.error("%s", exc)
        exit_code = 1
    except Exception:
        logger.exception("Ingestion command failed")
        exit_code = 1
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
