/*
  mart_executive_summary
  ----------------------
  One row per ticker showing: latest price, 1D/7D/30D returns, 30D volatility.
  This is the table Metabase's top-level dashboard reads directly.

  Lookback strategy for multi-period returns:
    - 1D  : comes straight from daily_return_pct in mart_daily_performance
    - 7D  : latest close vs. the most recent trading day on or before
            (latest_date - 7 calendar days)
    - 30D : same pattern but 30 calendar days back
  Using calendar days (not trading-day offsets like LAG(5)) means "7D" on a
  Monday includes the full prior weekend gap, which matches how retail
  investors read return figures.
  QUALIFY is Snowflake-specific syntax that filters a window result inline —
  it removes the need for a wrapping subquery and is much easier to read.
*/

with perf as (

    select * from {{ ref('mart_daily_performance') }}

),

-- Most recent trading day per ticker
latest as (

    select *
    from perf
    qualify row_number() over (partition by ticker order by price_date desc) = 1

),

-- Most recent price on or before (latest_date - 7 calendar days)
-- LEFT JOIN later so tickers with < 7 days of history still appear.
price_7d_ago as (

    select
        p.ticker,
        p.close_price as close_7d_ago
    from perf      p
    inner join latest l on p.ticker = l.ticker
    where p.price_date <= dateadd('day', -7, l.price_date)
    qualify row_number() over (partition by p.ticker order by p.price_date desc) = 1

),

-- Most recent price on or before (latest_date - 30 calendar days)
price_30d_ago as (

    select
        p.ticker,
        p.close_price as close_30d_ago
    from perf      p
    inner join latest l on p.ticker = l.ticker
    where p.price_date <= dateadd('day', -30, l.price_date)
    qualify row_number() over (partition by p.ticker order by p.price_date desc) = 1

),

final as (

    select
        l.ticker,
        l.ticker_name,

        -- JSE tickers trade in ZAR; US-listed ETFs and crypto trade in USD.
        -- This column prevents the "why is Naspers at 87,132?" confusion
        -- when the table is shown alongside SPY at 745.
        case
            when l.ticker in ('NPN.JO', 'MTN.JO', 'SBK.JO') then 'ZAR'
            else 'USD'
        end                                                             as price_currency,

        l.price_date                                                    as latest_price_date,
        l.close_price                                                   as latest_close,

        -- 1-day return is already computed in the upstream model
        round(l.daily_return_pct, 2)                                    as return_1d_pct,

        -- 7-day: NULL if fewer than 7 days of history exist
        case
            when p7.close_7d_ago is not null and p7.close_7d_ago != 0
            then round(
                (l.close_price - p7.close_7d_ago) / p7.close_7d_ago * 100,
                2
            )
        end                                                             as return_7d_pct,

        -- 30-day: NULL if fewer than 30 days of history exist
        case
            when p30.close_30d_ago is not null and p30.close_30d_ago != 0
            then round(
                (l.close_price - p30.close_30d_ago) / p30.close_30d_ago * 100,
                2
            )
        end                                                             as return_30d_pct,

        l.rolling_7d_avg_price,
        l.rolling_30d_volatility                                        as volatility_30d,

        current_timestamp()                                             as summary_generated_at

    from latest l
    left join price_7d_ago  p7  on l.ticker = p7.ticker
    left join price_30d_ago p30 on l.ticker = p30.ticker

)

select * from final
