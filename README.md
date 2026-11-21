# dataCollection Phase I ingestion

This repository contains the Phase I local ingestion foundation for NSE
cash-market EOD prices, NSE index CSV reports, an NSE security-master
snapshot, and mutual-fund schemes/NAV history from MFAPI.

The NSE exchange changes report formats and delivery locations. To avoid
quietly relying on a stale endpoint, the NSE adapters accept a local file
or an explicitly configured HTTP(S) URL. Verify the current official
report URL, CSV field names, historical availability, and terms before
configuring a remote feed. No NSE URL is assumed by default.

## Requirements

- Python 3.12+
- MySQL 8.0.16+ (required for enforced `CHECK` constraints)
- A current NSE report/reference file, or a confirmed URL to one
- Internet access for MFAPI and any configured remote source

## Local setup

1. Create and activate a virtual environment:

   ```sh
   python3.12 -m venv .venv
   source .venv/bin/activate
   ```

2. Install the project dependencies:

   ```sh
   python -m pip install -r requirements.txt
   ```

3. Create a local MySQL database and application user. Substitute a
   strong local password rather than copying the example:

   ```sql
   CREATE DATABASE indian_market
     CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
   CREATE USER 'data_ingest'@'127.0.0.1'
     IDENTIFIED BY 'replace-with-a-local-secret';
   GRANT ALL PRIVILEGES ON indian_market.* TO 'data_ingest'@'127.0.0.1';
   ```

4. Configure the environment:

   ```sh
   cp .env.example .env
   ```

   Update `DATABASE_URL` in `.env` to
   `mysql+pymysql://data_ingest:YOUR_PASSWORD@127.0.0.1:3306/indian_market?charset=utf8mb4`.
   URL-encode special characters in the password. The CLI and Alembic load
   `.env` for local development. Do not commit it. The raw source archive
   defaults to `./data/raw/`, which is ignored by Git.

5. Create or update the database schema:

   ```sh
   alembic upgrade head
   ```

   This creates the schema in MySQL; it does not copy existing data from
   a PostgreSQL database.

6. Confirm the application can connect by starting any ingestion command.
   The command tests database connectivity before contacting a source.

## Source inputs

### Nifty 500 security list (recommended before regular price imports)

Download the latest Nifty 500 constituent list from the NSE Indices
website:

```sh
python -m indian_market.jobs.download_nse500_security_master
```

This writes `current-nse-security-master.csv` in the current directory.
Use it to load the list into the database:

```sh
python -m indian_market.jobs.daily_pipeline \
  --source nse-master \
  --date YYYY-MM-DD \
  --input current-nse-security-master.csv
```

The date is the effective-from date for the snapshot. This is the Nifty
500 constituent list, not the complete list of all NSE-listed securities.
The downloader validates the CSV and requires at least 500 valid
constituents before saving it.

### NSE cash-market EOD

Download and validate the official NSE cash-market report for a trading
date:

```sh
python -m indian_market.jobs.download_nse_eod --date 2026-11-20
```

By default, this saves `nse-eod-20261120.csv.zip` in the current
directory. Choose another path with `--output`. The downloader checks the
ZIP/CSV format and confirms report dates match the requested date before
saving.

Pass the downloaded report to the ingestion command:

```sh
python -m indian_market.jobs.daily_pipeline \
  --source nse-eod \
  --date 2026-11-20 \
  --input nse-eod-20261120.csv.zip
```

For a date range, edit `from_date` and `to_date` at the top of
`indian_market/jobs/nse-eod-data-insert.sh` (or set `FROM_DATE` and
`TO_DATE` environment variables), then run:

```sh
bash indian_market/jobs/nse-eod-data-insert.sh
```

The script processes the inclusive calendar-date range in order and
deletes each temporary download after ingestion. It stops on the first
unexpected download or ingestion error. Weekends and holidays with no
published report are skipped. Partial ingestion runs are reported as
warnings and do not stop the remaining dates.

The parser supports CSV and ZIP containing exactly one CSV. It
normalizes current-style fields such as `TradDt`, `TckrSymb`, `SctySrs`,
`OpnPric`, `HghPric`, `LwPric`, `ClsPric`, `TtlTradgVol`, and
`TtlTrfVal`, plus common legacy `SYMBOL`/`OPEN`/`TOTTRDQTY` names. If a
future report has different headers or semantics, ingestion fails
explicitly until its parser is updated.

For a confirmed dated URL, configure a template in `.env`:

```dotenv
NSE_EOD_URL_TEMPLATE=https://confirmed-source.example/reports/{date}.csv.zip
```

`{date}` is replaced by `YYYYMMDD`; the date supplied on the command
line is still checked against each report record.

### NSE index history

The index adapter expects CSV fields corresponding to index name/date
and close. Open/high/low are optional. Example header aliases include
`Index Name`, `Index Date`, `Open Index Value`, `High Index Value`,
`Low Index Value`, and `Closing Index Value`.

```sh
python -m indian_market.jobs.daily_pipeline \
  --source nse-index \
  --date 2026-11-19 \
  --input /path/to/nse-index-report.csv
```

Or set `NSE_INDEX_URL_TEMPLATE` after confirming the source URL pattern.
An index name is normalized to a stable lowercase ID (for example
`NIFTY 50` becomes `nifty_50`). Review the input file's contents and
trade date before loading it.

### MFAPI scheme search and NAV history

Search for schemes and save the returned metadata locally:

```sh
python -m indian_market.jobs.daily_pipeline \
  --source mfapi-search \
  --query 'HDFC index'
```

Load full history for a scheme code:

```sh
python -m indian_market.jobs.daily_pipeline \
  --source mfapi \
  --scheme-code 125497
```

Multiple scheme codes can be supplied by repeating the option. MFAPI
history is stored as fund NAV observations; ETF exchange prices are a
separate dataset and are not treated as NAV.

### Combined run

After confirming and configuring both NSE URL templates, run the NSE
price and index jobs and optionally one or more MFAPI schemes:

```sh
python -m indian_market.jobs.daily_pipeline \
  --source all \
  --date 2026-11-19 \
  --scheme-code 125497
```

The combined command returns a non-zero exit status if a required
dataset fails or is partial. For manually downloaded files, run each
source command with its `--input`.

## What is persisted

- `security_master`: exchange-specific symbol/series mappings, ISIN and
  effective dates
- `daily_prices`: original unadjusted EOD OHLCV and traded value
- `index_master`, `index_prices`: normalized index identities and
  observations
- `mutual_fund_schemes`, `mutual_fund_nav`: MFAPI scheme metadata and
  historical NAVs
- `data_ingestion_log`: source, run status, row counts, raw-file path,
  checksum, timestamps, and failure summary
- `ingestion_issues`: blocking errors and missing-reference findings

Data uses unique natural keys and upserts so rerunning an input does not
create duplicate observations. Original source payloads are archived
under the configured raw-data directory by SHA-256 checksum. Stored
prices are unadjusted; corporate-action adjustments and analytics are
outside Phase I.

## Validation and run statuses

- Parser errors, invalid prices/NAVs, duplicate keys, and date mismatch
  fail the run; accepted records are not silently fabricated.
- Missing securities from an already populated active security master
  are recorded as issues and make the EOD run `partial`. A first load
  without a reference universe cannot reliably measure missing
  instruments.
- A partial or failed run exits non-zero. Close-to-close moves at or
  above `PRICE_MOVE_ALERT_PERCENT` (default `0.30`, or 30%) are recorded
  as issues but are not filtered out automatically.
- HTTP(S) reads have configured timeouts and bounded retries. Secrets
  must not be put into source URLs because source query strings are
  intentionally removed from error messages, but the configured
  `DATABASE_URL` must still be kept private.

Run history can be inspected with SQL:

```sql
SELECT source, dataset, requested_as_of, started_at, finished_at,
       status, counts, raw_file, error_summary
FROM data_ingestion_log
ORDER BY started_at DESC;
```

## Development checks

```sh
pytest
```

Parser tests use fixed fixtures and SQLite-backed persistence tests;
they do not call live NSE or MFAPI endpoints. A local MySQL instance is still required for actual local deployment.

## Scheduling and historical backfill

Run the single-source command manually for a known report first. Daily
jobs require a verified exchange trading date; do not substitute
“yesterday” or assume weekdays are trading days. Once the source
publication schedule and a reliable trading calendar are selected, use
the operating-system scheduler to invoke the same CLI with that date.
The CLI accepts explicit dates and can be called for historical dates
where source files are available. Rate limits and provider terms apply
to MFAPI and remote sources.

## Current Phase I limitations

- BSE coverage, Sensex, company fundamentals, announcements, corporate
  actions, analytics, scanner, dashboard, and MCP are not implemented.
- NSE source URLs and report access terms are intentionally
  configuration-driven and must be confirmed against current official
  exchange documentation.
- A ten-year history depends on source availability and is not
  guaranteed by this implementation.
