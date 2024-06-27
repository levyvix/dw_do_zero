WITH source as (

    SELECT * from {{ source('yfinance', 'commodities') }}
),
renamed as (
    SELECT
        "Date"::DATE as date,
        "Open"::FLOAT as open,
        "High"::FLOAT as high,
        "Low"::FLOAT as low,
        "Close"::FLOAT as close,
        "Volume"::FLOAT as volume,
        "Dividends"::FLOAT as dividends,
        "Stock Splits"::FLOAT as stock_splits,
        symbol
    FROM source)

SELECT * FROM renamed