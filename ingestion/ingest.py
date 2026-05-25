"""
ingest.py — AfriMarket Pulse
Pulls OHLCV price data from yfinance and USD/African-currency exchange rates
from open.er-api.com, then loads both into Snowflake RAW schema.

Usage:
    python ingest.py              # pulls last 7 days (use for daily runs)
    python ingest.py --historical # pulls last 365 days (use on first run)

Design principle: every run is idempotent. Re-running on the same day never
creates duplicate rows — the MERGE statement silently skips rows that already
exist. This is the foundation of a reliable production pipeline.
"""

import argparse
import logging
import os
import uuid
from datetime import datetime, date, timezone

import requests
import yfinance as yf
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Each entry maps a ticker symbol to a human-readable name that we store in
# the warehouse so dashboards never show raw symbols to end users.
TICKERS: dict[str, str] = {
    "NPN.JO":  "Naspers",
    "MTN.JO":  "MTN Group",
    "SBK.JO":  "Standard Bank",
    "EZA":     "iShares MSCI South Africa ETF",
    "AFK":     "VanEck Africa Index ETF",
    # NGE (Global X MSCI Nigeria ETF) delisted — removed 2026-05-25
    "SPY":     "S&P 500 ETF",
    "GLD":     "Gold ETF",
    "BTC-USD": "Bitcoin USD",
}

# Five African currencies to track against USD
CURRENCIES: list[str] = ["ZAR", "EGP", "NGN", "KES", "ETB"]

FX_API_URL = "https://open.er-api.com/v6/latest/USD"

# Default lookback for daily runs — 7 days ensures we always capture the most
# recent trading day even on weekends or after holidays.
DEFAULT_LOOKBACK_DAYS = 7
HISTORICAL_LOOKBACK_DAYS = 365


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------

def get_snowflake_conn() -> snowflake.connector.SnowflakeConnection:
    """
    Build a Snowflake connection from environment variables.

    We use snowflake-connector-python directly (not SQLAlchemy) because it
    gives us access to Snowflake-specific features like MERGE and temp tables
    without an ORM layer adding complexity.
    """
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            "Copy .env.example to .env and fill in your Snowflake credentials."
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="AFRIMARKET_PULSE",
        schema="RAW",
    )


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

def ensure_tables_exist(conn: snowflake.connector.SnowflakeConnection) -> None:
    """
    Create RAW tables if they don't already exist.

    Why run DDL in the ingestion script rather than a migration tool?
    At this stage of the project, keeping DDL here makes it self-contained —
    a single script bootstraps everything. In production you'd manage schema
    changes through a migration tool (Flyway, Liquibase, or dbt's
    --full-refresh flag).

    Why NOT NULL + UNIQUE constraints?
    The UNIQUE constraint on (TICKER, DATE) is what makes our MERGE idempotent.
    Without it, a bug in the pipeline could silently insert duplicates.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.PRICE_HISTORY (
                TICKER        VARCHAR(20)    NOT NULL,
                TICKER_NAME   VARCHAR(100),
                DATE          DATE           NOT NULL,
                OPEN          FLOAT,
                HIGH          FLOAT,
                LOW           FLOAT,
                CLOSE         FLOAT,
                VOLUME        BIGINT,
                INGESTED_AT   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
                CONSTRAINT uq_price_history UNIQUE (TICKER, DATE)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.EXCHANGE_RATES (
                BASE_CURRENCY   VARCHAR(10)   NOT NULL,
                QUOTE_CURRENCY  VARCHAR(10)   NOT NULL,
                RATE            FLOAT         NOT NULL,
                RATE_DATE       DATE          NOT NULL,
                INGESTED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                CONSTRAINT uq_exchange_rates UNIQUE (BASE_CURRENCY, QUOTE_CURRENCY, RATE_DATE)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.INGESTION_LOG (
                RUN_ID          VARCHAR(36)    NOT NULL PRIMARY KEY,
                SOURCE          VARCHAR(100),
                RECORDS_LOADED  INTEGER,
                STATUS          VARCHAR(20),
                ERROR_MESSAGE   VARCHAR(4000),
                STARTED_AT      TIMESTAMP_NTZ,
                COMPLETED_AT    TIMESTAMP_NTZ
            )
        """)
        conn.commit()
        log.info("RAW tables verified / created.")
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Extract: price data
# ---------------------------------------------------------------------------

def fetch_price_data(lookback_days: int) -> list[dict]:
    """
    Download OHLCV data for every ticker using yfinance.

    Why iterate ticker-by-ticker instead of using yf.download() in batch?
    yf.download() with multiple tickers returns a MultiIndex DataFrame whose
    column structure changes depending on whether you pass 1 ticker or N. That
    shape instability makes parsing error-prone. Individual Ticker.history()
    calls return a consistent single-level DataFrame every time, at the cost
    of a few extra HTTP requests — acceptable for 9 tickers.

    auto_adjust=True means yfinance applies corporate action adjustments
    (splits, dividends) to the OHLCV data automatically, so we always work
    with comparable prices across the full date range.
    """
    period = f"{lookback_days}d"
    log.info(f"Fetching {period} of price data for {len(TICKERS)} tickers...")

    rows: list[dict] = []
    for symbol, name in TICKERS.items():
        try:
            hist = yf.Ticker(symbol).history(period=period, auto_adjust=True)
            if hist.empty:
                log.warning(f"  {symbol}: no data returned (market may be closed or ticker invalid)")
                continue

            for ts, row in hist.iterrows():
                # yfinance returns a timezone-aware DatetimeIndex; .date() strips the tz
                rows.append({
                    "ticker":      symbol,
                    "ticker_name": name,
                    "date":        ts.date(),
                    "open":        float(row["Open"])   if row["Open"]   == row["Open"] else None,
                    "high":        float(row["High"])   if row["High"]   == row["High"] else None,
                    "low":         float(row["Low"])    if row["Low"]    == row["Low"]  else None,
                    "close":       float(row["Close"])  if row["Close"]  == row["Close"] else None,
                    # Volume is 0 for assets like BTC that don't report exchange volume
                    "volume":      int(row["Volume"]) if row["Volume"] == row["Volume"] else None,
                })
            log.info(f"  {symbol}: {len(hist)} rows fetched")

        except Exception as exc:
            # Log and continue — one bad ticker shouldn't abort the entire run
            log.warning(f"  {symbol}: fetch failed — {exc}")

    log.info(f"Price extraction complete. Total rows: {len(rows)}")
    return rows


# ---------------------------------------------------------------------------
# Extract: exchange rates
# ---------------------------------------------------------------------------

def fetch_exchange_rates() -> list[dict]:
    """
    Pull live USD exchange rates for African currencies from open.er-api.com.

    Why this API? It's free, requires no API key, and is reliable enough for
    a daily analytics pipeline. At production scale you'd use a paid provider
    (XE, OANDA) with historical data going back years. For our purposes —
    showing currency impact on African asset returns — daily spot rates are
    sufficient.
    """
    log.info("Fetching exchange rates from open.er-api.com...")
    resp = requests.get(FX_API_URL, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("result") != "success":
        raise RuntimeError(f"Exchange rate API returned non-success: {payload}")

    today = date.today()
    rows: list[dict] = []
    for currency in CURRENCIES:
        rate = payload["rates"].get(currency)
        if rate is None:
            log.warning(f"  Rate not found for {currency} — skipping")
            continue
        rows.append({
            "base_currency":  "USD",
            "quote_currency": currency,
            "rate":           float(rate),
            "rate_date":      today,
        })
        log.info(f"  USD/{currency}: {rate}")

    log.info(f"FX extraction complete. Total rows: {len(rows)}")
    return rows


# ---------------------------------------------------------------------------
# Load: upsert into Snowflake
# ---------------------------------------------------------------------------

def upsert_price_history(
    conn: snowflake.connector.SnowflakeConnection,
    rows: list[dict],
) -> int:
    """
    Load price rows into RAW.PRICE_HISTORY using a temp-table MERGE pattern.

    Why MERGE instead of INSERT?
    INSERT would create duplicates if the script runs more than once per day.
    MERGE (aka upsert) checks whether a row with the same (TICKER, DATE) already
    exists: if yes, skip it; if no, insert it. This makes every run idempotent —
    safe to retry without manual cleanup.

    Why stage through a temp table first?
    Snowflake's MERGE syntax requires a source *table or subquery*, not a
    VALUES list. We batch all rows into a session-scoped temp table, then issue
    a single MERGE statement. One MERGE > N individual checks.

    Returns the number of rows actually inserted (existing rows count as 0).
    """
    if not rows:
        log.info("No price rows to load.")
        return 0

    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TEMPORARY TABLE TEMP_PRICE (
                TICKER        VARCHAR(20),
                TICKER_NAME   VARCHAR(100),
                DATE          DATE,
                OPEN          FLOAT,
                HIGH          FLOAT,
                LOW           FLOAT,
                CLOSE         FLOAT,
                VOLUME        BIGINT
            )
        """)

        cursor.executemany(
            """
            INSERT INTO TEMP_PRICE (TICKER, TICKER_NAME, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (r["ticker"], r["ticker_name"], r["date"],
                 r["open"], r["high"], r["low"], r["close"], r["volume"])
                for r in rows
            ],
        )

        cursor.execute("""
            MERGE INTO RAW.PRICE_HISTORY AS target
            USING TEMP_PRICE AS source
                ON target.TICKER = source.TICKER
               AND target.DATE   = source.DATE
            WHEN NOT MATCHED THEN
                INSERT (TICKER, TICKER_NAME, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, INGESTED_AT)
                VALUES (source.TICKER, source.TICKER_NAME, source.DATE,
                        source.OPEN, source.HIGH, source.LOW, source.CLOSE,
                        source.VOLUME, CURRENT_TIMESTAMP())
        """)

        inserted = cursor.rowcount
        conn.commit()
        log.info(f"PRICE_HISTORY: {inserted} new rows inserted ({len(rows) - inserted} already existed).")
        return inserted

    finally:
        cursor.close()


def upsert_exchange_rates(
    conn: snowflake.connector.SnowflakeConnection,
    rows: list[dict],
) -> int:
    """
    Load exchange rate rows into RAW.EXCHANGE_RATES using the same MERGE pattern.
    Idempotent on (BASE_CURRENCY, QUOTE_CURRENCY, RATE_DATE).
    """
    if not rows:
        log.info("No FX rows to load.")
        return 0

    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TEMPORARY TABLE TEMP_FX (
                BASE_CURRENCY   VARCHAR(10),
                QUOTE_CURRENCY  VARCHAR(10),
                RATE            FLOAT,
                RATE_DATE       DATE
            )
        """)

        cursor.executemany(
            """
            INSERT INTO TEMP_FX (BASE_CURRENCY, QUOTE_CURRENCY, RATE, RATE_DATE)
            VALUES (%s, %s, %s, %s)
            """,
            [(r["base_currency"], r["quote_currency"], r["rate"], r["rate_date"])
             for r in rows],
        )

        cursor.execute("""
            MERGE INTO RAW.EXCHANGE_RATES AS target
            USING TEMP_FX AS source
                ON target.BASE_CURRENCY  = source.BASE_CURRENCY
               AND target.QUOTE_CURRENCY = source.QUOTE_CURRENCY
               AND target.RATE_DATE      = source.RATE_DATE
            WHEN NOT MATCHED THEN
                INSERT (BASE_CURRENCY, QUOTE_CURRENCY, RATE, RATE_DATE, INGESTED_AT)
                VALUES (source.BASE_CURRENCY, source.QUOTE_CURRENCY,
                        source.RATE, source.RATE_DATE, CURRENT_TIMESTAMP())
        """)

        inserted = cursor.rowcount
        conn.commit()
        log.info(f"EXCHANGE_RATES: {inserted} new rows inserted ({len(rows) - inserted} already existed).")
        return inserted

    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Observability: ingestion log
# ---------------------------------------------------------------------------

def write_ingestion_log(
    conn: snowflake.connector.SnowflakeConnection,
    run_id: str,
    source: str,
    records_loaded: int,
    status: str,
    error_message: str | None,
    started_at: datetime,
    completed_at: datetime,
) -> None:
    """
    Write one row to RAW.INGESTION_LOG per source per run.

    Why log inside the warehouse (not just to stdout)?
    Stdout disappears when the terminal closes. A log table in Snowflake is
    queryable — you can build a Metabase alert that fires when STATUS = 'error',
    or track record counts over time to detect data source drift. This is the
    difference between a script and an observable pipeline.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO RAW.INGESTION_LOG
                (RUN_ID, SOURCE, RECORDS_LOADED, STATUS, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, source, records_loaded, status, error_message, started_at, completed_at),
        )
        conn.commit()
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(lookback_days: int) -> None:
    """
    Main pipeline: connect → ensure schema → extract → load → log.

    Each source (price data, FX rates) gets its own run_id so failures are
    attributable at the source level. If price ingestion succeeds but FX fails,
    the log shows exactly which source broke — no guessing.
    """
    log.info(f"Starting AfriMarket Pulse ingestion (lookback: {lookback_days} days)")
    conn = get_snowflake_conn()

    try:
        ensure_tables_exist(conn)

        # --- Source 1: yfinance price data ---
        price_run_id = str(uuid.uuid4())
        price_started = datetime.now(timezone.utc)
        price_status = "error"
        price_loaded = 0
        price_error: str | None = None

        try:
            price_rows = fetch_price_data(lookback_days)
            price_loaded = upsert_price_history(conn, price_rows)
            price_status = "success"
        except Exception as exc:
            price_error = str(exc)
            log.error(f"Price ingestion failed: {exc}")

        write_ingestion_log(
            conn, price_run_id, "yfinance",
            price_loaded, price_status, price_error,
            price_started, datetime.now(timezone.utc),
        )

        # --- Source 2: open.er-api.com exchange rates ---
        fx_run_id = str(uuid.uuid4())
        fx_started = datetime.now(timezone.utc)
        fx_status = "error"
        fx_loaded = 0
        fx_error: str | None = None

        try:
            fx_rows = fetch_exchange_rates()
            fx_loaded = upsert_exchange_rates(conn, fx_rows)
            fx_status = "success"
        except Exception as exc:
            fx_error = str(exc)
            log.error(f"FX ingestion failed: {exc}")

        write_ingestion_log(
            conn, fx_run_id, "open.er-api.com",
            fx_loaded, fx_status, fx_error,
            fx_started, datetime.now(timezone.utc),
        )

        log.info(
            f"Ingestion complete. "
            f"Prices: {price_status} ({price_loaded} rows). "
            f"FX: {fx_status} ({fx_loaded} rows)."
        )

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AfriMarket Pulse — Snowflake ingestion")
    parser.add_argument(
        "--historical",
        action="store_true",
        help=f"Pull {HISTORICAL_LOOKBACK_DAYS} days of history (use on first run only)",
    )
    args = parser.parse_args()

    days = HISTORICAL_LOOKBACK_DAYS if args.historical else DEFAULT_LOOKBACK_DAYS
    run(lookback_days=days)
