with source as(

    select * from {{ source('yfinance', 'movimentacao') }}
),

renamed as (
    select
        "date" as data_movimentacao,
        "symbol" as symbol,
        "action" as acao,
        "quantity" as quantidade
    from source
)

select * from renamed