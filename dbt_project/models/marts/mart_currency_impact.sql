/*
  mart_currency_impact
  --------------------
  Converts all asset prices to USD so JSE-listed stocks (priced in ZAR)
  can be compared directly against US-listed ETFs and benchmarks.

  The problem this solves:
    Naspers trades at ~87,000 ZAR. SPY trades at ~745 USD.
    Without currency conversion those numbers are incomparable.
    Dividing by the ZAR/USD rate puts everything on the same scale.

  Join strategy:
    We only have FX data from when the pipeline started running.
    For any price_date that has no exact FX rate match (weekends,
    dates before pipeline started), we use the most recent available
    rate on or before that date. QUALIFY + ROW_NUMBER handles this
    cleanly without a subquery.
*/

with prices as (

    select * from {{ ref('stg_price_history') }}

),

fx_rates as (

    select * from {{ ref('stg_exchange_rates') }}
    where base_currency = 'USD'

),

-- For each price_date, find the most recent ZAR rate on or before that date.
-- This handles weekends and dates where FX data wasn't collected yet.
zar_rates as (

    select
        p.price_date,
        f.rate     as zar_rate,
        f.rate_date as fx_rate_date
    from (select distinct price_date from prices) p
    left join fx_rates f
        on f.quote_currency = 'ZAR'
        and f.rate_date <= p.price_date
    qualify row_number() over (
        partition by p.price_date
        order by f.rate_date desc
    ) = 1

),

final as (

    select
        p.ticker,
        p.ticker_name,
        p.price_date,

        -- Original price in native currency
        case
            when p.ticker in ('NPN.JO', 'MTN.JO', 'SBK.JO') then 'ZAR'
            else 'USD'
        end                                                 as original_currency,

        p.close_price                                       as original_close,

        -- USD-adjusted price for apples-to-apples comparison
        -- ZAR tickers: divide by ZAR rate (e.g. 87,000 ZAR / 18.5 = ~4,700 USD)
        -- USD tickers: price unchanged
        case
            when p.ticker in ('NPN.JO', 'MTN.JO', 'SBK.JO')
            then round(p.close_price / z.zar_rate, 4)
            else p.close_price
        end                                                 as close_price_usd,

        z.zar_rate,
        z.fx_rate_date

    from prices     p
    left join zar_rates z on p.price_date = z.price_date

)

select * from final
