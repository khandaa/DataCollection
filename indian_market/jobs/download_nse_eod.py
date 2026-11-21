from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import httpx

from indian_market.ingestion.common import (
    IngestionError,
    SourceNotFoundError,
    fetch_payload,
)
from indian_market.ingestion.nse_eod import parse_nse_eod

NSE_EOD_URL_TEMPLATE = (
    "https://archives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)
logger = logging.getLogger("indian_market.nse_eod_download")


def download_nse_eod(
    trade_date: date,
    output_path: Path | None = None,
    *,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    client: httpx.Client | None = None,
) -> tuple[Path, int]:
    dated_string = trade_date.strftime("%Y%m%d")
    source = NSE_EOD_URL_TEMPLATE.format(date=dated_string)
    payload = fetch_payload(
        source,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        client=client,
    )
    records = parse_nse_eod(payload, trade_date)

    destination = output_path or Path(f"nse-eod-{dated_string}.csv.zip")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    return destination, len(records)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(
        description="Download and validate an NSE cash-market EOD report."
    )
    parser.add_argument(
        "--date",
        required=True,
        type=date.fromisoformat,
        help="NSE trading date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output ZIP path (default: nse-eod-YYYYMMDD.csv.zip)",
    )
    args = parser.parse_args()
    try:
        output_path, count = download_nse_eod(args.date, args.output)
    except SourceNotFoundError as exc:
        logger.info("No NSE EOD report for %s: %s", args.date, exc)
        raise SystemExit(2) from exc
    except (IngestionError, OSError) as exc:
        logger.error("NSE EOD download failed: %s", exc)
        raise SystemExit(1) from exc
    print(f"Downloaded and validated {count} EOD rows to {output_path}")


if __name__ == "__main__":
    main()
