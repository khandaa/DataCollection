# Phase I Implementation Guide: Local Market Data Ingestion

This guide turns Phase I of the project requirements into an ordered
implementation plan for a local Python ingestion engine and MySQL
database.

## 1. Phase I outcome

At the end of Phase I, a local, repeatable pipeline should:

- Download and retain NSE cash-market EOD reports and the selected NSE
  index data source.
- Parse and normalize those reports into MySQL.
- Map exchange records to a security master, retaining ISIN and
  exchange-specific symbols where available.
- Detect duplicate, missing, unmapped, and invalid records before data
  is treated as complete.
- Make imports safe to rerun and record source, retrieval/as-of times,
  row counts, status, and errors.

Phase I is complete when the acceptance checklist at the end of this
guide passes for a repeatable sample and a scheduled daily run.

## 2. Scope boundaries

### Included

- Python project/environment and local configuration.
- Local MySQL setup and schema migrations.
- Minimal ingestion run log and retained raw source files.
- NSE EOD prices and index observations.
- Security master import and exchange-record mapping.
- Parsing, normalization, validation, upserts, tests, and a local
  command-line entry point.

### Deferred to later phases

- BSE market-data ingestion and complete NSE/BSE reconciliation.
- Fundamentals, shareholding, corporate actions, and announcements.
- Adjusted prices, analytics, scanner scores, dashboard, and MCP server.
- Cloud deployment or paid data subscriptions.
- MFApi data

Design extension points for these later sources, but do not let them
block Phase I. Use NSE's official reports as the primary bulk EOD source;
the NSE MCP may help investigate or cross-check supported data, but is
not a substitute for repeatable bulk ingestion. Verify current report
formats, source availability, rate limits, usage terms, and historical
depth before implementation. Do not assume that public access grants
redistribution rights.

## 3. Phase I data flow

Implement a single pipeline in this order:

1. Select the requested trading date .
2. Fetch the source response with timeouts and bounded retries.
3. Store the original file/response unchanged in a local raw-data
   directory.
4. Parse it into typed source records.
5. Normalize field names, dates, numeric values, and identifiers.
6. Resolve each NSE instrument against the security master.
7. Validate records and report errors/warnings.
8. Upsert valid rows in a database transaction.
9. Record run status, row counts, source location, and timestamps.
10. Mark a market-data date complete only when required inputs have
    arrived and passed validation.

Do not silently drop malformed rows, invent identifiers, fill missing
prices, or treat a failed/partial run as successful. Keep unusual but
parseable market moves and flag them for review rather than deleting
them automatically.

## 4. Recommended minimal project layout

```text
indian_market/
├── config/
│   ├── settings.py
│   └── sources.yaml
├── database/
│   ├── connection.py
│   ├── models.py
│   └── migrations/
├── ingestion/
│   ├── common.py
│   ├── nse_eod.py
│   ├── nse_indices.py
├── jobs/
│   └── daily_pipeline.py
├── tests/
│   ├── fixtures/
│   ├── test_nse_eod.py
│   ├── test_indices.py
│   └── test_database_upserts.py
├── data/
│   └── raw/                 # ignored by version control
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

Keep network access, parsing, validation, and database persistence in
separate functions/modules so source-format changes do not affect the
database contract. Store configuration and secrets outside source
control. Pin dependency versions after the initial workflow is tested.

## 5. Step-by-step implementation

### Step 1 — Confirm source contracts

Before writing parsers:

1. Inspect the current NSE EOD report(s) needed for cash-market OHLCV
   and confirm the exact report used for daily stock prices.
2. Identify the official source/report for the indices in the initial
   scope. Start with a defined list, such as Nifty 50 and Nifty 500;
   include Sensex only if its source is available in this NSE-first
   phase.
3. Download representative reports for a recent normal trading day,
   an unusual day if available, and at least one older date.
4. Record report URL/source identifier, format, filename pattern,
   expected fields, exchange trade-date meaning, and access/usage
   constraints in `sources.yaml` or the README.

Use the official exchange report for repeatable file ingestion. Keep a
small parser fixture in tests; do not make automated tests depend on a
live exchange website.

### Step 2 — Create the local Python environment

1. Create a Python 3.12+ virtual environment.
2. Add only the Phase I dependencies: a MySQL driver,
   SQLAlchemy, a migration tool, HTTPX or Requests, and Pandas/NumPy if
   required by the chosen parser. Avoid installing Phase II-IV packages
   yet.
3. Add a configuration loader for the database URL, raw-data path,
   request timeouts, retry limits, and source settings.
4. Provide `.env.example` with variable names and dummy values only;
   ignore actual `.env` files and raw downloaded data in version
   control.
5. Add a CLI entry point that can run one source/date on demand, for
   example `python -m jobs.daily_pipeline --source nse --date YYYY-MM-DD`.

The command should exit non-zero on a failed or incomplete ingestion and
print a concise run summary without exposing credentials.

### Step 3 — Install and verify MySQL

1. Install MySQL 8.0.16+ locally using the platform's standard package or
   installer.
2. Create a dedicated local database and a non-superuser application
   role with only the permissions needed to run migrations and ingest
   data.
3. Configure the connection string through an environment variable.
4. Verify the app can connect, begin/rollback a transaction, and read
   the database version.
5. Document local startup, database creation, migration, and shutdown
   commands in the README.

Use standard MySQL for Phase I.

### Step 4 — Define the minimal schema and migrations

Create migrations for these Phase I tables:

| Table | Phase I contents | Suggested key |
|---|---|---|
| `security_master` | Internal security ID, exchange, ISIN, symbol, series, name, effective-from/to dates, active status, source, last-seen timestamp | Internal ID; index ISIN and exchange/symbol/series/effective dates |
| `daily_prices` | Security ID, trade date, raw OHLC, volume, traded value if supplied, source, source file, ingestion timestamp | Security ID + trade date |
| `data_ingestion_log` | Source, dataset, requested date/range, start/end time, status, counts, raw-file/response reference, error summary | Run ID |

Store prices and NAVs as raw source values. Preserve each exchange's
symbol and series; use ISIN as a stable identifier when available.
Represent security-symbol mappings with effective dates so symbol
changes or reuse do not overwrite historical identity. Preserve the
source's date rather than substituting the download date. Represent
unavailable fields as null and record missing-data flags or validation
issues rather than deriving values implicitly.

The source file reference can be a relative path under the raw-data
directory plus a checksum. The log should distinguish at least
`started`, `succeeded`, `partial`, and `failed`. Add database constraints
and indexes for natural keys, positive NAV/price values, and common
date/security lookups.

### Step 5 — Implement raw-file and run-log handling

1. Save each downloaded NSE file before parsing, with source and
   retrieval date in its path/name.
3. Never overwrite an earlier raw response; a repeated fetch should
   create a distinct retrieval record or use a content-addressed file
   name.
4. Create a run-log row before each source job starts.
5. On completion, update status, timestamps, downloaded/parsed/inserted/
   updated/rejected counts, and a concise error summary.
6. Keep exception details in application logs, but never log database
   credentials or secret headers.

If persistence fails after parsing, record the run as failed and leave
the raw input available for replay.

### Step 6 — Build the NSE security master and mapping

1. Obtain the official security/instrument reference file used by the
   selected NSE report, if available.
2. Parse it into the `security_master` table before loading prices.
3. Normalize whitespace and field representation without changing the
   source symbol's identity or discarding the original value.
4. Map each EOD row to one security using exchange, symbol, and series;
   attach ISIN where present in the reference data.
5. Record the source and effective/last-seen date of each mapping.
6. Send ambiguous or unmapped rows to a rejected-record report/table;
   do not guess based on company name.
7. Detect symbol/series changes and retain the mapping history needed
   to interpret prior prices rather than rewriting old observations.

If the source does not supply enough information to map an instrument
unambiguously, report the limitation and keep that record out of the
trusted price table until the mapping is resolved.

### Step 7 — Implement and test NSE EOD ingestion

Separate the job into fetch, archive, parse, normalize, map, validate,
and upsert operations.

Validate at minimum:

- Required columns exist and the report date matches the requested
  exchange trade date.
- Every record has an exchange symbol and series and resolves to one
  security-master record.
- No duplicate natural keys occur within the input.
- OHLC values are numeric and positive; low does not exceed high; open
  and close lie within the reported high/low range where the source
  semantics support those checks.
- Volume and traded value are non-negative when present.
- Missing expected securities are measured against the active
  reference universe for that date, not against weekdays or a static
  present-day universe.
- Unusually large price changes are flagged for review, not
  automatically discarded.

Persist only rows that pass blocking checks. Use a transaction and
`INSERT ... ON CONFLICT`/equivalent upserts so rerunning the same
source/date updates the same logical observations without duplicates.
Do not mark the date complete if a required report is absent, parsing
fails, or unresolved/missing records exceed the explicitly configured
acceptance threshold. Initially, treat unresolved security mappings as
blocking for the affected dataset and make the threshold visible in the
run report.

### Step 10 — Add shared retry and observability behavior

- Set explicit connect/read timeouts for every HTTP request.
- Retry transient network errors and appropriate server responses with
  bounded exponential backoff; do not retry permanent client errors
  indefinitely.
- Respect source limits and terms; avoid concurrent requests unless
  supported.
- Include source, dataset, requested date/scheme, run ID, duration, and
  outcome in structured logs.
- Emit counts for downloaded, parsed, accepted, inserted, updated,
  rejected, duplicate, unmapped, and missing records.
- Keep a dataset freshness/as-of timestamp separate from ingestion
  time.
- Make a failed source's status visible; the overall pipeline must not
  report success when a required source has failed.

### Step 11 — Orchestrate and schedule locally

Create `daily_pipeline.py` to run the NSE EOD file, configured index
datasets, and configured MFAPI updates in a defined order. It should:

1. Use an exchange calendar or an explicit trading-date input rather
   than assuming every weekday is a trading day.
2. Run individual jobs independently enough to identify which dataset
   failed, but return an overall failure if a required dataset did not
   complete.
3. Support manual historical backfill with bounded date ranges and
   progress summaries.
4. Schedule only after manual runs are reliable, using cron,
   APScheduler, or the operating system scheduler.
5. Avoid hard-coding a fixed publication time as a guarantee; reports
   may arrive later than the target schedule in the requirements.

Start with a manual command for a single known date and a small set of
fund schemes. Add unattended daily scheduling after the retry,
idempotency, and failure-reporting checks pass.


## 6. Suggested execution order and deliverables

| Order | Work | Deliverable / verification |
|---|---|---|
| 1 | Confirm current NSE  source contracts | Documented sample files/responses and terms/format notes |
| 2 | Create Python environment, configuration, and CLI shell | Clean setup and a non-zero failure exit path |
| 3 | Install MySQL and create the app user/database | Successful local connection |
| 4 | Add migrations for Phase I tables and constraints | Fresh database migrates successfully |
| 5 | Add raw archive and ingestion run log | Raw inputs retained; run outcome and counts queryable |
| 6 | Load security reference data and implement mapping | Representative NSE instruments resolve unambiguously |
| 7 | Implement NSE EOD parser, validation, and upsert | A known report imports and can be safely replayed |
| 10 | Add pipeline orchestration, tests, and documentation | Manual run passes end-to-end; failures are explicit |
| 11 | Schedule local run | Run history, freshness, and failures are inspectable |

## 7. Definition of done

Phase I is done only when all of the following are true:

- [ ] A current supported NSE EOD file can be downloaded, archived,
  parsed, and imported repeatably.
- [ ] Exchange symbols and series map to security-master records;
  ISINs are retained wherever supplied, and unresolved mappings are
  reported rather than guessed.
- [ ] Duplicate source keys, missing expected securities, and malformed
  or out-of-range values are detected and included in a run report.
- [ ] The same source/date can be imported twice without duplicate
  logical observations.
- [ ] Every stored dataset row or dataset batch can be traced to its
  source and as-of date, with retrieval/ingestion timestamps.
- [ ] Raw inputs remain available for parser replay.
- [ ] Failed, partial, and successful runs are distinguishable; a
  required-source failure cannot appear as a successful daily run.
- [ ] Automated parser, validation, and database-upsert tests pass.
- [ ] A documented local manual run and scheduled run have been
  exercised.

## 8. Phase I limitations to carry forward

- NSE-only equity ingestion is not complete Indian-market coverage.
  BSE-only securities and complete Sensex coverage remain out of scope
  until the next phase.
- NSE MCP historical availability described by the requirements may
  not meet the ten-year target and is not the bulk-ingestion source.
- Do not assume all historical reports are available or adjusted for
  corporate actions. Phase I stores unadjusted source observations.
- Data availability, pricing, request limits, historical depth, and
  licensing must be rechecked before relying on or redistributing any
  provider data.
