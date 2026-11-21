#!/usr/bin/env bash
set -euo pipefail

from_date="${FROM_DATE:-2026-11-02}"
to_date="${TO_DATE:-2026-11-20}"
python="${PYTHON:-python}"

cd "$(dirname "$0")/../.."

dates=$(python3 - "$from_date" "$to_date" <<'PY'
from datetime import date, timedelta
import sys

try:
    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2])
except ValueError as exc:
    raise SystemExit(f"Dates must use YYYY-MM-DD format: {exc}")

if end < start:
    raise SystemExit("to_date must be on or after from_date")

current = start
while current <= end:
    print(current.isoformat())
    current += timedelta(days=1)
PY
)

download_file=""
processed_days=0
no_report_days=0
partial_days=0
cleanup() {
    if [[ -n "$download_file" && -f "$download_file" ]]; then
        rm -f -- "$download_file"
    fi
}
trap cleanup EXIT

while IFS= read -r process_date; do
    [[ -n "$process_date" ]] || continue
    date_compact="${process_date//-/}"
    download_file="nse-eod-${date_compact}-$$.csv.zip"
    rm -f -- "$download_file"

    echo "Downloading NSE EOD report for ${process_date}"
    if "$python" -m indian_market.jobs.download_nse_eod \
        --date "$process_date" \
        --output "$download_file"; then
        :
    else
        download_status=$?
        if [[ $download_status -eq 2 ]]; then
            echo "No report for ${process_date}; skipping (weekend/holiday or unavailable date)"
            rm -f -- "$download_file"
            download_file=""
            no_report_days=$((no_report_days + 1))
            continue
        fi
        exit "$download_status"
    fi

    echo "Ingesting NSE EOD report for ${process_date}"
    if "$python" -m indian_market.jobs.daily_pipeline \
        --source nse-eod \
        --date "$process_date" \
        --input "$download_file"; then
        :
    else
        ingest_status=$?
        if [[ $ingest_status -eq 2 ]]; then
            echo "Warning: ${process_date} ingested with partial status; continuing"
            partial_days=$((partial_days + 1))
        else
            exit "$ingest_status"
        fi
    fi

    rm -f -- "$download_file"
    download_file=""
    processed_days=$((processed_days + 1))
    echo "Done processing ${process_date}"
done <<< "$dates"

echo "Finished date range: ${processed_days} processed, ${no_report_days} without reports, ${partial_days} partial"
