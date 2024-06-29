with commodities as (

    select * from {{ ref("stg__commodities") }}
),
movimentacao as (

    select * from {{ ref("stg__movimentacao") }}
),
joined as (

    select
        c.date,
        c.symbol,
        c.valor_fechamento,
        m.acao,
        m.quantidade,
        (m.quantidade * c.valor_fechamento) as valor_total,
        case WHEN m.acao = 'sell' then (m.quantidade * c.valor_fechamento)
        else -(m.quantidade *c.valor_fechamento) end as ganho
    from commodities c
    INNER JOIN movimentacao m
    on c.symbol = m.symbol
),

last_day as (

    select max(date) as max_date from joined
),
filtered as (

    select
        *
    from
        joined
    where
        date = (select max_date from last_day)
)


SELECT * from filtered