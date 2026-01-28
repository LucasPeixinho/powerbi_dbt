with conciliacoes as (
    select * from {{ ref('int_conciliacoes') }}
),

transacoes_validas as (
    select
        id_transacao
    from
        conciliacoes
    group by
        id_transacao
    having
        sum(valor) = 0
),

final as (
    select
        c.id_transacao,
        c.historico, 
        c.data_transacao,
        c.data_conciliacao,
        c.valor,
        c.tipo_transacao
    from 
        conciliacoes c
    join
        transacoes_validas n
    on
        c.id_transacao = n.id_transacao
    where
        c.data_transacao > TO_DATE('01/01/2026', 'DD/MM/YYYY')
)

select * from final