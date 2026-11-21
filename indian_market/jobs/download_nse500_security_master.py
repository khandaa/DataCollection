from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import httpx

from indian_market.ingestion.common import IngestionError, fetch_payload
from indian_market.ingestion.nse_master import parse_nse_security_master

NIFTY_500_URL = (
    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
)
MINIMUM_CONSTITUENTS = 500
logger = logging.getLogger("indian_market.nse500_download")


def download_nse500_security_master(
    output_path: Path,
    *,
    timeout_seconds: float = 30,
    max_attempts: int = 4,
    client: httpx.Client | None = None,
) -> int:
    payload = fetch_payload(
        NIFTY_500_URL,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        client=client,
    )
    records = parse_nse_security_master(payload, date.today())
    if len(records) < MINIMUM_CONSTITUENTS:
        raise IngestionError(
            f"Expected at least {MINIMUM_CONSTITUENTS} Nifty 500 constituents; "
            f"received {len(records)}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)
    return len(records)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(
        description="Download the current Nifty 500 constituents CSV."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("current-nse-security-master.csv"),
        help="Output CSV path (default: current-nse-security-master.csv)",
    )
    args = parser.parse_args()
    try:
        count = download_nse500_security_master(args.output)
    except (IngestionError, OSError) as exc:
        logger.error("Nifty 500 download failed: %s", exc)
        raise SystemExit(1) from exc
    print(f"Downloaded {count} Nifty 500 constituents to {args.output}")


if __name__ == "__main__":
    main()
