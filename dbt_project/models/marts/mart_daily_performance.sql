/*
  mart_daily_performance
  ----------------------
  One row per (ticker, trading_date). Core model that powers most charts.

  Window function strategy:
    - daily_return_pct  : LAG(1) captures yesterday's close so we never
                          divide by zero on the first row (prev_close is NULL).
    - rolling_7d_avg    : 7-row window (6 preceding + current) gives a
                          price-smoothing line used in trend charts.
    - rolling_30d_vol   : STDDEV over 30 daily returns annualises well
                          and is the standard retail volatility measure.
*/

with price_data as (

    select * from {{ ref('stg_price_history') }}

),

-- Step 1: add the previous close using LAG.
-- Separating this into its own CTE means we reference prev_close_price
-- by name in the next step instead of repeating the window expression.
with_lag as (

    select
        ticker,
        ticker_name,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        lag(close_price) over (
            partition by ticker
            order by price_date
        ) as prev_close_price

    from price_data

),

-- Step 2: compute daily return from the lag column.
-- CASE guard: first row of each ticker has NULL prev_close → return is NULL,
-- not a divide-by-zero error.
with_returns as (

    select
        *,
        case
            when prev_close_price is not null and prev_close_price != 0
            then round(
                (close_price - prev_close_price) / prev_close_price * 100,
                4
            )
        end as daily_return_pct

    from with_lag

),

-- Step 3: add rolling window metrics.
-- ROWS BETWEEN N PRECEDING AND CURRENT ROW gives a trailing window —
-- it only looks backward, so there's no look-ahead bias in the numbers.
with_rolling as (

    select
        *,

        round(
            avg(close_price) over (
                partition by ticker
                order by price_date
                rows between 6 preceding and current row
            ),
            4
        ) as rolling_7d_avg_price,

        round(
            stddev(daily_return_pct) over (
                partition by ticker
                order by price_date
                rows between 29 preceding and current row
            ),
            4
        ) as rolling_30d_volatility

    from with_returns

)

select
    ticker,
    ticker_name,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    prev_close_price,
    daily_return_pct,
    rolling_7d_avg_price,
    rolling_30d_volatility

from with_rolling
