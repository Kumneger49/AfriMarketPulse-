with source as (

    select * from {{ source('raw', 'exchange_rates') }}

),

cleaned as (

    select
        base_currency,
        quote_currency,

        -- 6 decimal places for FX: rates like NGN can be 1500+
        -- and we may need sub-cent precision for smaller currencies.
        round(cast(rate as float), 6)   as rate,

        cast(rate_date as date)         as rate_date,

        ingested_at

    from source

    where rate is not null

)

select * from cleaned
