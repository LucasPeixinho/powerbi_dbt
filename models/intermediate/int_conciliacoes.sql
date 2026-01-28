with conciliacoes as (
    select * from {{ ref('stg_conciliacoes') }}
),

final as (
    select
        id_transacao,
        historico,
        data_transacao,
        data_conciliacao,
        tipo_transacao,
        case
            when
                tipo_transacao = 'C'
            then
                valor * -1
            else
                valor
        end as valor,
        id_funcionario
    from
        conciliacoes
)

select * from final