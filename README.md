# AfriMarket Pulse

A production-style analytics pipeline that tracks African and emerging market assets — JSE-listed equities, African ETFs, and global benchmarks — and serves a live executive dashboard updated every morning automatically.

Built as interview preparation for a Data Analytics role at Baraka, a YC-backed retail investment app targeting emerging markets.

---

## What It Does

Every night, the pipeline automatically:
1. Pulls fresh stock prices and currency exchange rates from the internet
2. Loads them into a cloud data warehouse (Snowflake)
3. Transforms the raw data into clean, analysis-ready tables
4. The dashboard reflects the new data the next morning

No manual work. No laptop required.

---

## The Stack

| Layer | Tool | What it does |
|---|---|---|
| Ingestion | Python + yfinance | Pulls daily stock prices and FX rates |
| Warehouse | Snowflake | Stores all data in three layers: RAW → STAGING → MARTS |
| Transform | dbt Cloud | Cleans and models the raw data into business-ready tables |
| Dashboard | Metabase Cloud | Visualises the final data for business users |
| Orchestration | GitHub Actions + dbt Cloud | Runs everything automatically every night |

---

## Data Sources

- **yfinance** — daily OHLCV prices for 8 assets:
  - JSE South Africa: Naspers (NPN.JO), MTN Group (MTN.JO), Standard Bank (SBK.JO)
  - African ETFs: iShares MSCI South Africa (EZA), VanEck Africa Index (AFK)
  - Global benchmarks: S&P 500 (SPY), Gold (GLD), Bitcoin (BTC-USD)
- **open.er-api.com** — daily USD exchange rates for ZAR, EGP, NGN, KES, ETB

---

## How the Data Flows

```
yfinance + open.er-api.com
        │
        ▼  GitHub Actions runs ingest.py every night at 11pm UTC
Snowflake RAW
  └── PRICE_HISTORY      raw daily prices, never modified
  └── EXCHANGE_RATES     raw FX rates, never modified
  └── INGESTION_LOG      one row per pipeline run for monitoring
        │
        ▼  dbt Cloud runs every night at midnight UTC
Snowflake STAGING
  └── stg_price_history      clean column names, correct data types
  └── stg_exchange_rates     clean FX rates
        │
        ▼
Snowflake MARTS
  └── mart_daily_performance    daily returns, 7d avg, 30d volatility
  └── mart_executive_summary    one row per asset, latest snapshot
        │
        ▼  Metabase Cloud queries MARTS directly
Executive Dashboard
```

---

## The Dashboard

Five charts, all pulling live from Snowflake:

1. **Market Overview** — latest price, 1D/7D/30D returns and volatility for every asset, color-coded green/red
2. **African Markets vs S&P 500** — full year indexed to 100 at start, shows relative performance
3. **30D Return by Asset** — bar chart ranking assets by 30-day return
4. **Volatility by Asset** — which assets carry the most risk over the past 30 days
5. **Correlation vs S&P 500** — which African assets move with or against the global benchmark

---

## Automated Schedule

```
11:00 PM UTC  →  GitHub Actions pulls fresh data into Snowflake RAW
12:00 AM UTC  →  dbt Cloud transforms RAW into MARTS and runs 19 data quality tests
Every morning →  Dashboard is up to date
```

---

## Local Setup

```bash
# 1. Clone the repo
git clone https://github.com/Kumneger49/AfriMarketPulse-.git
cd AfriMarketPulse-

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add credentials
cp .env.example .env
# Edit .env with your Snowflake credentials

# 5. Run historical ingestion (first time only)
python ingestion/ingest.py --historical

# 6. Run dbt transformations
dbt run --project-dir dbt_project --profiles-dir dbt_project
dbt test --project-dir dbt_project --profiles-dir dbt_project
```

---

## Project Structure

```
AfriMarket_Pulse/
  ingestion/
    ingest.py               pulls data from APIs, loads into Snowflake RAW
  dbt_project/
    dbt_project.yml         dbt configuration
    models/
      sources.yml           declares RAW tables as dbt sources
      staging/              views that clean and type the raw data
      marts/                tables with business logic and aggregations
      schema.yml            19 data quality tests
    macros/
      generate_schema_name  routes models to correct Snowflake schemas
  .github/
    workflows/
      ingest.yml            GitHub Actions daily ingestion schedule
  scripts/
    run_pipeline.sh         manual pipeline runner
  .env.example              credential template
  requirements.txt          Python dependencies
```
