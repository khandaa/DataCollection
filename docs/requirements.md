# Indian Market Data Connector Architecture

## Personal Investment Research and Stock Scanner

**Objective:** Build a personal Indian investment research platform
using daily end-of-day (EOD) data, Python, a historical database and a
custom MCP server connected to ChatGPT.

**Approach:** Free-first. Use official exchange reports and free
mutual-fund NAV APIs where practical. Pay for a licensed provider only
if it materially improves completeness, reliability or reduces
maintenance.

> **Important:** Provider capabilities, free-tier limits, pricing, API
> access and terms can change. Verify current documentation and
> licensing before relying on a source. Publicly accessible data is not
> automatically licensed for redistribution or commercial use.

------------------------------------------------------------------------

# 1. Requirements

  -----------------------------------------------------------------------
  Area                                Requirement
  ----------------------------------- -----------------------------------
  Purpose                             Personal investment research and
                                      stock scanner

  Frequency                           Daily EOD data

  Coverage                            NSE stocks, BSE stocks, Nifty and
                                      Sensex indices, mutual funds, ETFs,
                                      fundamentals and corporate
                                      announcements

  Integration                         Python + ChatGPT MCP

  Budget                              Prefer free; paid if significantly
                                      better

  Deployment                          Local machine initially; optional
                                      later migration to cloud

  History                             Target 10 years where available
  -----------------------------------------------------------------------

## Design principles

1.  Separate data ingestion from AI analysis.
2.  Store source data and provenance; do not rely on an AI conversation
    as the historical record.
3.  Keep original, unadjusted prices and separately calculate
    adjusted-price series.
4.  Use stable security identifiers (especially ISIN) as well as
    exchange symbols.
5.  Validate data before using it in analytics.
6.  Use read-only database access for the MCP server.
7.  Preserve historical financial-statement versions and filing
    timestamps for credible backtesting.
8.  Begin locally and avoid unnecessary cloud or paid services until the
    system is proven.

# 2. Recommended architecture

``` text
DATA SOURCES
  ├── NSE reports / Bhavcopy / NSE MCP
  ├── BSE reports / BSE data services
  ├── AMFI / MFAPI / MFNAV
  ├── Stoxim or another fundamentals provider
  └── Optional licensed provider (e.g. TrueData)
             |
             v
PYTHON INGESTION ENGINE
  ├── Download / API clients
  ├── Parsing and normalisation
  ├── Validation and reconciliation
  ├── Retry and logging
  └── Scheduled jobs
             |
             v
POSTGRESQL DATABASE
  ├── Security master, prices and indices
  ├── Mutual fund NAVs and ETF prices
  ├── Fundamentals and ratios
  ├── Shareholding and corporate actions
  └── Corporate announcements
             |
             v
ANALYTICS ENGINE
  ├── Momentum and relative strength
  ├── Fundamental quality and valuation
  ├── Technical indicators
  ├── Risk, rolling returns and drawdowns
  └── Scanner rankings
             |
             v
PYTHON MCP SERVER
             |
             v
CHATGPT INVESTMENT ASSISTANT
```

## Technology stack

  ------------------------------------------------------------------------
  Component             Suggested technology               Indicative cost
  --------------------- --------------------- ----------------------------
  Programming           Python 3.12+                                  Free

  Data processing       Pandas, NumPy                                 Free

  Database              MySQL 8.0.16+                    Free/self-hosted

  MCP framework         FastMCP or Python MCP             Free/open source
                        SDK                   

  HTTP clients          Requests, HTTPX                               Free

  Technical indicators  TA-Lib or pandas-ta        Free/open source; check
                                                             package terms

  Scheduling            Cron, Task Scheduler                          Free
                        or APScheduler        

  Dashboard             Streamlit                         Free/open source

  Initial hosting       Local laptop/desktop                No hosting fee

  Optional VPS          Cloud provider                        Indicatively
                                                 ₹500--1,500/month; verify
                                                                    prices
  ------------------------------------------------------------------------

A standard MySQL installation is sufficient initially.

# 3. Data providers and APIs

Use specialist sources for each data category. Do not assume one
provider offers complete, licensed and consistent coverage for
everything.

## 3.1 NSE stocks and indices

### NSE official MCP

-   Bhavcopy MCP: `https://mcp.nseindia.in/bhavcopy/cm/mcp`
-   Current Market MCP: `https://mcp.nseindia.in/cmmkt/mcp`
-   Documentation: <https://www.nseindia.com/nse-mcp>
-   Reports: <https://www.nseindia.com/all-reports>

Capabilities described in NSE's MCP material include daily/historical
market information, OHLCV, market breadth, gainers/losers, stock
comparisons and index information. The published service material
describes historical coverage of up to five years for relevant tools and
delayed current-market data.

**Use in this design:** Explore and query supported NSE market data, and
validate exchange-report interpretation.

**Limitations and checks:** - Do not assume historical series are
adjusted for splits, bonuses or dividends. - Confirm which instruments
and fields each tool covers. - Confirm availability, rate limits and
usage terms. - Do not treat MCP responses as a complete replacement for
repeatable bulk EOD ingestion. - The five-year coverage described for
some tools does not meet the proposed ten-year target by itself.

### NSE EOD reports / Bhavcopy

Use the official reports portal as the first source to investigate for
repeatable daily files. Build a parser for the exact report formats
currently published.

Implementation checks: - Confirm the current report format and file
naming. - Confirm whether the report covers the desired cash-market
universe. - Preserve exchange trade date and source filename. -
Reconcile record counts and symbols against the security master. -
Retain raw downloaded files so parsers can be rerun.

Official reports: <https://www.nseindia.com/all-reports>

## 3.2 BSE stocks and Sensex

### BSE official market-data services

-   Portal: <https://marketdata.bseindia.com/>

BSE offers data products covering exchange market information, EOD data,
indices and corporate information. Full or dependable bulk data access
may require a paid subscription.

Use this source to: - Source BSE EOD prices and Sensex/index data where
available. - Source BSE corporate announcements and filings. - Obtain
official/licensed BSE data if full BSE coverage is a hard requirement.

**Important:** NSE coverage does not equal complete BSE coverage. Some
securities may be BSE-only. Exchange-specific symbols, identifiers and
price series need reconciliation. Initially use available BSE reports
where permitted; obtain a licensed feed if complete BSE coverage cannot
be established reliably.

## 3.3 Mutual fund NAV

### MFAPI

-   Documentation: <https://www.mfapi.in/docs/>
-   Example search: `https://api.mfapi.in/mf/search?q=HDFC`
-   Example scheme history: `https://api.mfapi.in/mf/125497`

MFAPI provides scheme search/details and historical NAV observations
through a simple REST interface. It is a useful free-first source for
personal research.

Example Python:

``` python
import requests

search_url = "https://api.mfapi.in/mf/search?q=HDFC"
schemes = requests.get(search_url, timeout=30).json()

scheme_code = 125497
history_url = f"https://api.mfapi.in/mf/{scheme_code}"
history = requests.get(history_url, timeout=30).json()

print(history.get("meta"))
print(history.get("data", [])[:5])
```

### MFNAV

-   API documentation: <https://mfnav.in/api-docs>

Use as a secondary source for scheme search, NAV history and
cross-checking. Check current rate limits, terms and fields before
production use.

### Mutual fund data design

Store AMFI scheme code, scheme name/category, ISINs where supplied, NAV
date, NAV value, source and retrieval timestamp.

Do not confuse a mutual fund's NAV with an ETF's exchange-traded price.

## 3.4 ETFs

For ETFs such as NiftyBeES, MONQ50 and gold ETFs, collect
exchange-traded instrument data separately from mutual fund NAV.

Recommended fields: - Exchange and symbol. - ISIN. - Trade date. - Open,
high, low, close. - Volume and traded value. - NAV/iNAV, where reliably
available. - Premium/discount to NAV, calculated only when the NAV/iNAV
timestamp and methodology are suitable.

ETF market price should come from exchange EOD data. The fund NAV source
may provide a separate NAV observation.

## 3.5 Fundamentals and corporate announcements

### Stoxim

-   Website/API information: <https://www.stoxim.com/>

Stoxim advertises Indian company financial data such as financial
statements, ratios, shareholding and corporate information. The earlier
review identified a published free tier of 500 API requests/day and a
Starter price of ₹2,999/month for 10,000 requests/day. **Treat these
figures as indicative and re-check the current pricing page and plan
terms before budgeting.**

Recommended evaluation: 1. Register for the free tier. 2. Test 20--30
companies across large-, mid- and small-cap segments. 3. Check annual
and quarterly P&L, balance sheet and cash-flow depth. 4. Check
historical availability and whether restated/revised results are
represented. 5. Check ROE, ROCE, debt, margins and valuation-ratio
definitions. 6. Check shareholding pattern and filing dates. 7. Check
corporate-action and announcement coverage. 8. Record API limits, update
frequency, missing fields and licensing restrictions.

Do not treat advertised coverage as verified until tested.

### TrueData

-   Market data API: <https://www.truedata.in/products/marketdataapi>

TrueData is a commercial alternative to investigate for consolidated
NSE/BSE market data, indices and other market datasets. API access and
pricing should be requested separately; a delayed Workspace product does
not automatically include API access.

Evaluate it if: - Free exchange-report ingestion proves unreliable or
too labour-intensive. - Complete BSE coverage is required. - You need
one vendor for both NSE and BSE EOD data. - The vendor can confirm
historical depth and licensing.

Do not buy real-time features for an EOD-only workflow unless there is a
separate need.

### Official exchange corporate disclosures

Use the relevant NSE and BSE corporate-announcement and
financial-results portals as authoritative filing sources. Retain
original filing date/time, exchange, company identifier, document link
and retrieval timestamp. Stoxim may supplement structured data, but
should not silently replace the original exchange filing record.

# 4. Provider comparison

  ----------------------------------------------------------------------------------------------------------------------------
  Provider           NSE prices    BSE prices             Indices      MF NAV              Fundamentals /      Cost approach
                                                                                           announcements       
  ------------------ ------------- ---------------------- ------------ ------------------- ------------------- ---------------
  NSE MCP            Yes,          No                     NSE indices  No                  Limited             Free access as
                     supported                                                                                 documented
                     tools                                                                                     

  NSE                Yes           No                     NSE reports  No                  No                  Public reports;
  reports/Bhavcopy                                                                                             check terms

  BSE official       No            Yes                    Sensex/BSE   No                  Corporate data      Paid products
  services                                                indices                                              may apply

  MFAPI              No            No                     No           Yes                 Scheme metadata     Free

  MFNAV              No            No                     No           Yes                 Scheme metadata     Free access
                                                                                                               subject to
                                                                                                               terms/limits

  Stoxim             Not a primary Not a primary price    Not primary  Not primary         Advertised coverage Free tier /
                     price feed    feed                                                                        paid tier

  TrueData           Yes           Yes                    Yes          Product-dependent   Product-dependent   Commercial;
                                                                                                               request API
                                                                                                               quote

  Yahoo Finance /    Useful for    Partial/inconsistent   Some indices Limited             Partial             Free,
  yfinance           prototyping                                                                               unofficial; not
                                                                                                               authoritative
  ----------------------------------------------------------------------------------------------------------------------------

**Recommended starting combination:** - NSE: official reports/Bhavcopy
and NSE MCP for supported query workflows. - BSE: official reports where
accessible and permitted; licensed data if full coverage is required. -
Indices: official NSE and BSE index data. - Mutual funds: MFAPI plus
MFNAV cross-checks. - Fundamentals: test Stoxim. - Corporate
announcements: official exchange disclosures, with provider
supplementation if useful. - Paid fallback: evaluate TrueData for market
data after confirming EOD API price, historical depth, completeness and
licensing.

# 5. Database design

Use MySQL with a schema that separates instrument identity,
observations, financial statements and events.

  ------------------------------------------------------------------------
  Table                    Main contents           Refresh cadence
  ------------------------ ----------------------- -----------------------
  `security_master`        ISIN, exchange, symbol, Daily / as changed
                           company name, series,   
                           sector                  

  `daily_prices`           Trade date, OHLC,       Daily
                           close, volume, traded   
                           value                   

  `index_prices`           Index identifier, date, Daily
                           OHLC and close          

  `mutual_fund_schemes`    Scheme code, name,      Daily / as changed
                           category, ISIN          

  `mutual_fund_nav`        Scheme code, NAV date,  Daily
                           NAV                     

  `etf_prices`             ETF prices, volume,     Daily
                           traded value            

  `fundamentals`           P&L, balance sheet,     Quarterly / annual
                           cash flow               

  `financial_ratios`       PE, PB, ROE, ROCE,      Quarterly /
                           margins, leverage       recalculated

  `shareholding`           Promoter, FII, DII and  Quarterly
                           public holdings         

  `corporate_actions`      Dividends, splits,      As announced
                           bonuses, rights         

  `announcements`          Filing, category,       Daily
                           timestamp, source and   
                           document reference      

  `technical_indicators`   RSI, MACD, moving       Daily
                           averages, momentum      

  `scanner_results`        Date, universe, score   Daily
                           components and rank     

  `data_ingestion_log`     Source, job, run time,  Every run
                           status, row counts,     
                           errors                  
  ------------------------------------------------------------------------

## Database rules

1.  Use ISIN as a key wherever possible; retain exchange-specific
    symbols.
2.  Store exchange and series because the same security can have
    different exchange records.
3.  Store raw/unadjusted OHLCV.
4.  Create a separate adjusted-price calculation layer for splits and
    bonuses; document whether dividends are included.
5.  Store both filing timestamp and financial period end date.
6.  Preserve original financial statement versions and restatements.
7.  Record source, source file/API endpoint and ingestion timestamp.
8.  Make imports idempotent using unique keys and upserts.
9.  Keep raw source files so a parser can be rerun.
10. Store missing-data flags rather than silently filling missing
    values.

Suggested unique keys: - Daily equity price:
`(exchange, ISIN, trade_date, series)`. - Index price:
`(index_id, trade_date)`. - Mutual fund NAV:
`(scheme_code, nav_date)`. - Corporate announcement: source-specific
filing identifier, with a fallback composite key. - Financial statement:
`(company_id, period_end, statement_type, filing_version)`.

# 6. Daily ingestion schedule

Suggested target schedule in Indian Standard Time. These are operational
targets, not promises about when each exchange or fund publishes data.

  -----------------------------------------------------------------------
  Target time             Job                     Work
  ----------------------- ----------------------- -----------------------
  4:30 PM                 NSE/BSE prices          Download available EOD
                                                  reports and index data

  5:00 PM                 Validation              Check missing
                                                  securities, duplicates,
                                                  zero/abnormal values
                                                  and counts

  6:00 PM                 Corporate data          Retrieve announcements,
                                                  corporate actions and
                                                  newly published results

  9:30 PM                 Mutual funds            Retrieve NAV updates
                                                  and compare against
                                                  previous observations

  10:00 PM                Analytics               Recalculate indicators,
                                                  returns and rankings

  10:15 PM                MCP ready               Publish refreshed
                                                  scanner and analytics
                                                  data
  -----------------------------------------------------------------------

## Retry and data-quality strategy

-   Retry failed downloads with exponential backoff.
-   Do not mark a trading date complete until expected source files have
    arrived and passed validation.
-   Use an exchange calendar rather than assuming every weekday is a
    trading day.
-   Check duplicate records and unexpected missing symbols.
-   Compare a sample of closing prices against a second source.
-   Alert on unusually large price moves, but do not automatically
    delete them.
-   Maintain a data freshness timestamp for every dataset.
-   Do not run a final scanner if essential price data is incomplete;
    flag it as provisional.

# 7. Python project structure

``` text
indian_market/
├── config/
│   ├── settings.py
│   └── sources.yaml
├── ingestion/
│   ├── nse_bhavcopy.py
│   ├── bse_data.py
│   ├── mfapi.py
│   ├── fundamentals.py
│   └── announcements.py
├── database/
│   ├── models.py
│   ├── connection.py
│   └── migrations/
├── analytics/
│   ├── momentum.py
│   ├── technicals.py
│   ├── quality.py
│   ├── valuation.py
│   ├── rolling_returns.py
│   └── drawdown.py
├── scanner/
│   ├── momentum50.py
│   └── top20.py
├── mcp/
│   └── server.py
├── dashboard/
│   └── app.py
├── jobs/
│   └── daily_pipeline.py
├── tests/
│   ├── test_ingestion.py
│   ├── test_prices.py
│   └── test_analytics.py
├── requirements.txt
└── README.md
```

## Initial Python dependencies

``` bash
pip install pandas numpy requests httpx sqlalchemy pymysql fastmcp streamlit
```

Pin tested package versions in `requirements.txt` once stable. Keep
credentials in environment variables or a secrets manager; do not commit
API keys or database passwords to source control.

# 8. MCP server design

The MCP server should expose predictable, typed, read-only tools. It
should query the local database and precomputed analytics tables rather
than scrape exchange websites on every ChatGPT request.

## Proposed MCP tools

  Tool                       Purpose
  -------------------------- ----------------------------------------------
  `get_stock_quote`          Latest available EOD quote
  `get_stock_history`        Historical OHLCV
  `get_stock_fundamentals`   Financial statements
  `get_stock_ratios`         PE, PB, ROE, ROCE and other ratios
  `get_corporate_actions`    Dividends, splits, bonuses
  `get_announcements`        Recent company disclosures
  `get_index_data`           Nifty, Sensex and other indices
  `get_mf_nav`               Mutual fund NAV history
  `compare_mutual_funds`     Compare schemes and return measures
  `get_etf_data`             ETF price and volume
  `get_momentum50`           Momentum 50 ranking
  `get_top20_stocks`         Current Top-20 scanner output
  `get_rolling_returns`      Rolling returns for stocks, funds or indices
  `get_max_drawdown`         Historical maximum drawdown
  `get_portfolio_analysis`   Portfolio-level analytics

## Simplified FastMCP example

This example assumes the MySQL tables already exist. It is a starter,
not a complete production implementation.

``` python
from fastmcp import FastMCP
from sqlalchemy import create_engine, text
import os

mcp = FastMCP("Indian Market Research")

engine = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True
)

@mcp.tool()
def get_stock_quote(symbol: str) -> dict:
    """Get the latest available EOD stock price."""
    sql = text("""
        SELECT symbol, trade_date, open,
               high, low, close, volume
        FROM daily_prices
        WHERE symbol = :symbol
        ORDER BY trade_date DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(
            sql, {"symbol": symbol}
        ).mappings().first()

    return dict(result) if result else {
        "error": "Stock not found"
    }


@mcp.tool()
def get_top20_stocks() -> list:
    """Return the latest Top 20 momentum stocks."""
    sql = text("""
        SELECT symbol, score, momentum,
               quality, valuation
        FROM scanner_results
        WHERE trade_date = (
            SELECT MAX(trade_date)
            FROM scanner_results
        )
        ORDER BY score DESC
        LIMIT 20
    """)

    with engine.connect() as conn:
        return [
            dict(row)
            for row in conn.execute(sql).mappings()
        ]


@mcp.tool()
def get_mf_nav(scheme_code: int) -> list:
    """Get the latest 30 NAV observations."""
    sql = text("""
        SELECT nav_date, nav
        FROM mutual_fund_nav
        WHERE scheme_code = :scheme
        ORDER BY nav_date DESC
        LIMIT 30
    """)

    with engine.connect() as conn:
        return [
            dict(row)
            for row in conn.execute(
                sql, {"scheme": scheme_code}
            ).mappings()
        ]


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
```

## Production hardening required

-   Use a dedicated read-only database user.
-   Validate and constrain all tool arguments.
-   Use parameterised SQL.
-   Apply query timeouts and result limits.
-   Authenticate the MCP endpoint.
-   Use HTTPS for remote access.
-   Avoid exposing unrestricted SQL.
-   Return source and as-of timestamps with market data.
-   Clearly label delayed, stale or incomplete datasets.
-   Log requests and errors without logging secrets.

## ChatGPT connection

Two possible routes:

**A. Direct NSE MCP:** Connect the official NSE MCP directly if the
user's ChatGPT plan and connector interface support the required custom
MCP connection.

**B. Custom Python MCP:** Host the Python MCP server at an HTTPS
endpoint and register it through ChatGPT's supported custom
connector/developer configuration, where available.

The custom Python MCP is the preferred long-term option because it can
expose the user's database, scanner scores and portfolio calculations
consistently.

Plan and account availability for custom MCP connectors should be
checked in current ChatGPT documentation. Local development can proceed
independently of ChatGPT connector availability.

# 9. Investment analytics and stock scanner

The proposed scanner uses a 100-point score.

  ------------------------------------------------------------------------
  Component                                   Weight Inputs
  --------------------- ---------------------------- ---------------------
  Momentum                                        30 1M, 3M, 6M and 12M
                                                     returns, relative
                                                     strength and trend

  Fundamental quality                             30 Revenue/profit
                                                     growth, ROE, ROCE,
                                                     debt and cash flow

  Valuation                                       20 PE, PB, PEG,
                                                     EV/EBITDA and
                                                     historical valuation

  Technical strength                              10 Moving averages, RSI,
                                                     MACD and breakout
                                                     strength

  Risk and liquidity                              10 Volatility, drawdown,
                                                     traded value and
                                                     liquidity

  **Total**                                  **100** 
  ------------------------------------------------------------------------

Scores should be normalised within comparable peer groups where
appropriate. Banks and manufacturing companies, for example, should not
be assessed using identical leverage or valuation thresholds.

## Scanner outputs

Recommended daily columns:

  Column            Description
  ----------------- ---------------------------------------
  Rank              Current rank within selected universe
  Stock             Company/security name
  Symbol            Exchange symbol
  ISIN              Stable security identifier
  Score             Total score out of 100
  Momentum          Momentum sub-score
  Quality           Fundamental quality sub-score
  Valuation         Valuation sub-score
  Technical         Technical sub-score
  Risk/Liquidity    Risk and liquidity sub-score
  1M return         One-month return
  3M return         Three-month return
  6M return         Six-month return
  12M return        Twelve-month return
  Breakout status   Defined technical breakout condition
  Data date         Latest price date
  Data quality      Complete, provisional or stale

Maintain separate rankings for: - Nifty 50. - Nifty 500. - Nifty Midcap
150. - Nifty Smallcap 250. - Momentum 50. - Custom portfolio universe.

Store historical universe membership if the scanner will be backtested.
Using today's constituents to simulate historical performance creates
survivorship bias.

## Mutual fund analytics

Calculate: - 1Y, 3Y, 5Y and 10Y CAGR where history allows. - Rolling 1Y,
3Y and 5Y returns. - Maximum drawdown. - Volatility and downside
deviation. - Benchmark-relative returns. - Consistency of
outperformance. - Recovery time after drawdowns.

Compare funds with an appropriate benchmark and category. Handle
Direct/Regular and Growth/IDCW plan distinctions correctly.

## Index analytics

Calculate: - Nifty 50, Nifty 500 and Sensex returns. - Nifty Midcap and
Smallcap relative strength. - Sector-index performance. - Market breadth
and advances/declines where data is available. - Rolling returns and
drawdowns.

# 10. Costs and upgrade strategy

## Free-first estimate

  Item                                        Initial cost                    Monthly cost
  ---------------------- --------------------------------- -------------------------------
  Python and libraries                                  ₹0                              ₹0
  NSE reports/MCP          ₹0 for documented public access                              ₹0
  MFAPI/MFNAV                ₹0 for documented free access                              ₹0
  Stoxim free tier                                      ₹0   ₹0 within current plan limits
  MySQL local                                           ₹0                              ₹0
  Local scheduling                                      ₹0                              ₹0
  Streamlit local                                       ₹0                              ₹0
  Optional VPS                                         ---        Indicatively ₹500--1,500
  Paid data provider                        Quote required                  Quote required

The ₹0 approach has trade-offs: more engineering effort, less guaranteed
completeness, possible source-format changes and no assured service
level.

## When to pay

Consider a paid provider only when: 1. Exchange reports are too
difficult to maintain. 2. Complete BSE coverage is required and free
access is insufficient. 3. Historical data depth is inadequate. 4.
Missing or inconsistent data materially affects scanner results. 5. A
vendor provides clear licensing, documentation, support and service
commitments.

TrueData is a candidate to evaluate for consolidated NSE/BSE market
data. Ask specifically for: - EOD-only API pricing. - Ten-year
historical depth by exchange and instrument. - Corporate-action-adjusted
data methodology. - Index coverage. - BSE coverage and exclusions. - API
rate limits and availability. - Personal-use and redistribution
rights. - Sample files and a trial.

Do not purchase real-time feeds for an EOD-only system unless
requirements change.

# 11. Implementation roadmap

  ------------------------------------------------------------------------
  Phase                             Estimated effort Deliverables
  --------------------- ---------------------------- ---------------------
  1\. Market data                          3--4 days Python environment,
  infrastructure                                       MySQL, security
                                                     master, NSE EOD and
                                                     index ingestion,
                                                     MFAPI

  2\. Fundamentals and                     2--4 days Stoxim evaluation,
  corporate data                                     financial statements,
                                                     announcements,
                                                     corporate actions and
                                                     validation

  3\. Analytics and                        3--5 days 100-point score,
  scanner                                            technical indicators,
                                                     Top-20, rolling
                                                     returns and drawdowns

  4\. MCP and ChatGPT                      2--3 days Python MCP tools,
  integration                                        authentication,
                                                     connection and tests
  ------------------------------------------------------------------------

These are indicative prototype estimates for one developer. Provider
access, data-format changes and historical backfills may extend the
schedule.

## Phase 1 acceptance criteria

-   Daily EOD file can be downloaded and parsed repeatably.
-   Security identifiers map correctly to exchange instruments.
-   Duplicate and missing records are detected.
-   Historical data is stored idempotently.
-   MF NAV history is loaded and validated.
-   Every dataset has a source and as-of timestamp.

## Phase 2 acceptance criteria

-   A representative sample of companies has financial statements.
-   Financial periods and filing dates are correctly represented.
-   Corporate actions are retained.
-   Exchange disclosures can be located from stored records.
-   Missing provider fields are reported, not silently inferred.

## Phase 3 acceptance criteria

-   Scanner output is reproducible for a given data snapshot.
-   All scores can be explained by component scores.
-   Returns use correctly adjusted prices where appropriate.
-   Universe membership is historically tracked for backtesting.
-   Incomplete data is clearly flagged.

## Phase 4 acceptance criteria

-   MCP tools return correct data for known test securities.
-   MCP is read-only.
-   Authentication and HTTPS are configured for remote use.
-   Responses include data source and as-of date.
-   ChatGPT can retrieve scanner results and ask follow-up questions.

# 12. Example questions for the finished ChatGPT assistant

## Stock research

-   Find the 20 highest-scoring stocks in Momentum 50.
-   Which stocks have crossed their 200-day moving average?
-   Compare Laurus Labs, Newgen and Affle on valuation and growth.
-   Find stocks with rising earnings and improving price momentum.

## Mutual funds

-   Compare Invesco Midcap, Bandhan Small Cap and Nippon Smallcap Index.
-   Calculate rolling three-year returns.
-   Show maximum drawdown and recovery period.
-   Compare active mutual funds against their appropriate benchmarks.

## Indices

-   Compare Nifty 50, Nifty 500 and Sensex over 1, 3 and 5 years.
-   Calculate market breadth.
-   Find the strongest sector indices.
-   Compare Nifty Midcap and Smallcap relative strength.

## Portfolio

-   Monitor my holdings.
-   Identify stocks with deteriorating fundamentals.
-   Flag unusually high drawdowns.
-   Generate a daily portfolio health report.

# 13. Final recommendation

Start with this combination:

  -----------------------------------------------------------------------
  Requirement                         Recommended source
  ----------------------------------- -----------------------------------
  NSE stock prices                    NSE official EOD reports/Bhavcopy

  NSE market queries                  NSE MCP

  BSE stock prices                    BSE official reports initially;
                                      licensed feed if full coverage is
                                      required

  Nifty indices                       NSE official reports/MCP

  Sensex indices                      BSE data

  Mutual fund NAV                     MFAPI + MFNAV cross-check

  ETF prices                          Exchange EOD prices

  Fundamentals                        Stoxim free-tier trial

  Corporate announcements             Official NSE/BSE disclosures,
                                      supplemented by provider data

  Database                            MySQL

  Analytics                           Python, Pandas and NumPy

  MCP                                 Custom Python MCP server

  Dashboard                           Streamlit
  -----------------------------------------------------------------------

**Recommended build sequence:** Develop locally, ingest NSE and mutual
fund data first, validate Stoxim fundamentals on a representative
sample, then add BSE coverage and the scanner. Build the MCP layer after
the database and analytics outputs are stable.

This produces reusable personal investment research infrastructure
rather than a dashboard that depends on live third-party queries every
time it is opened.

# 14. Reference links

## Exchanges

-   NSE MCP: <https://www.nseindia.com/nse-mcp>
-   NSE reports: <https://www.nseindia.com/all-reports>
-   BSE Market Data Services: <https://marketdata.bseindia.com/>

## Mutual funds

-   MFAPI documentation: <https://www.mfapi.in/docs/>
-   MFNAV API documentation: <https://mfnav.in/api-docs>

## Fundamentals and market data

-   Stoxim: <https://www.stoxim.com/>
-   TrueData Market Data API:
    <https://www.truedata.in/products/marketdataapi>

## Development

-   FastMCP: <https://gofastmcp.com/>
-   MySQL: <https://www.mysql.com/>
-   Pandas: <https://pandas.pydata.org/>
-   Streamlit: <https://streamlit.io/>
