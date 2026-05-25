with source as (

    select * from {{ source('raw', 'price_history') }}

),

cleaned as (

    select
        ticker,
        ticker_name,

        -- Explicit cast to DATE so downstream models never receive a TIMESTAMP
        cast(date as date)               as price_date,

        -- ROUND here so every layer above works with clean 4dp numbers.
        -- RAW keeps the full float so we never lose precision at source.
        round(cast(open  as float), 4)   as open_price,
        round(cast(high  as float), 4)   as high_price,
        round(cast(low   as float), 4)   as low_price,
        round(cast(close as float), 4)   as close_price,

        cast(volume as bigint)           as volume,

        ingested_at

    from source

    -- Rows with a null close cannot be used for return calculations.
    -- Volume = 0 rows are corporate-action placeholders, not real trading days.
    where close is not null
      and volume > 0

)

select * from cleaned
