-- AfriMarket Pulse — Dashboard SQL Queries
-- Paste each one into Metabase's native SQL editor (New Question > SQL Query)
-- All queries target AFRIMARKET_PULSE database, MARTS or STAGING schema.

-- ============================================================
-- CHART 1: Market Overview Table
-- Visualization: Table  |  Color-code return columns green/red
-- ============================================================
SELECT
    ticker_name                         AS "Asset",
    ticker                              AS "Ticker",
    latest_close                        AS "Latest Price",
    return_1d_pct                       AS "1D Return %",
    return_7d_pct                       AS "7D Return %",
    return_30d_pct                      AS "30D Return %",
    volatility_30d                      AS "30D Volatility",
    latest_price_date                   AS "As Of"
FROM AFRIMARKET_PULSE.MARTS.MART_EXECUTIVE_SUMMARY
ORDER BY return_1d_pct DESC NULLS LAST;


-- ============================================================
-- CHART 2: African Markets vs S&P 500 — Indexed to 100
-- Visualization: Line chart  |  X: price_date  |  Y: indexed_price
-- Series: ticker_name  |  Shows relative performance from same start
-- ============================================================
WITH last_90_days AS (

    SELECT
        ticker,
        ticker_name,
        price_date,
        close_price
    FROM AFRIMARKET_PULSE.MARTS.MART_DAILY_PERFORMANCE
    WHERE price_date >= DATEADD('day', -90, CURRENT_DATE())
      AND ticker IN ('NPN.JO', 'MTN.JO', 'EZA', 'AFK', 'NGE', 'SPY')

),

base_prices AS (

    SELECT
        ticker,
        ticker_name,
        price_date,
        close_price,
        -- FIRST_VALUE grabs the earliest close in our 90-day window.
        -- Dividing every day's close by that gives an index starting at 1.0,
        -- multiplied by 100 so it reads as "100 = starting point".
        FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker
            ORDER BY price_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS start_price

    FROM last_90_days

)

SELECT
    price_date                                      AS "Date",
    ticker_name                                     AS "Asset",
    ROUND(close_price / start_price * 100, 2)       AS "Indexed Price (Base=100)"
FROM base_prices
ORDER BY price_date, ticker_name;


-- ============================================================
-- CHART 3: Currency Pulse — 30-Day FX Trend
-- Visualization: Line chart  |  X: rate_date  |  Y: rate
-- Series: quote_currency  |  One line per African currency
-- ============================================================
SELECT
    rate_date                           AS "Date",
    quote_currency                      AS "Currency",
    rate                                AS "USD Rate"
FROM AFRIMARKET_PULSE.STAGING.STG_EXCHANGE_RATES
WHERE rate_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY rate_date, quote_currency;


-- ============================================================
-- CHART 4: Volatility by Asset
-- Visualization: Bar chart  |  X: Asset  |  Y: 30D Volatility
-- Sort descending — highest risk assets on the left
-- ============================================================
SELECT
    ticker_name                         AS "Asset",
    ROUND(volatility_30d, 4)            AS "30D Volatility (Daily Return Std Dev)",
    latest_price_date                   AS "As Of"
FROM AFRIMARKET_PULSE.MARTS.MART_EXECUTIVE_SUMMARY
WHERE volatility_30d IS NOT NULL
ORDER BY volatility_30d DESC;


-- ============================================================
-- CHART 5: Correlation vs S&P 500 (trailing 90 days)
-- Visualization: Bar chart  |  X: Asset  |  Y: Correlation
-- 1.0 = moves in perfect lockstep with SPY
-- 0.0 = no relationship
-- Negative = moves opposite to SPY (natural hedge)
-- ============================================================
WITH daily_returns AS (

    SELECT
        price_date,
        ticker,
        ticker_name,
        daily_return_pct
    FROM AFRIMARKET_PULSE.MARTS.MART_DAILY_PERFORMANCE
    WHERE price_date >= DATEADD('day', -90, CURRENT_DATE())
      AND daily_return_pct IS NOT NULL

),

spy_returns AS (

    SELECT
        price_date,
        daily_return_pct AS spy_return
    FROM daily_returns
    WHERE ticker = 'SPY'

),

-- Join every non-SPY asset's daily return to SPY's return on the same date.
-- We need matching dates because CORR() aggregates across rows.
paired AS (

    SELECT
        a.ticker_name,
        a.daily_return_pct  AS asset_return,
        s.spy_return
    FROM daily_returns  a
    INNER JOIN spy_returns s ON a.price_date = s.price_date
    WHERE a.ticker NOT IN ('SPY')

)

SELECT
    ticker_name                             AS "Asset",
    ROUND(CORR(asset_return, spy_return), 3) AS "Correlation vs SPY"
FROM paired
GROUP BY ticker_name
ORDER BY "Correlation vs SPY" DESC;
