with source as (
    select * from {{ source('cedep', 'pcmov') }}
),

renamed as (
    select
        NUMTRANSVENDA as id_transacao_venda,
        CODPROD as id_produto,
        CODFILIAL as id_filial, 
        NUMTRANSENT as id_transacao_entrada,
        DTMOV as data_movimento,
        CODOPER as codigo_operacao,
        QT as quantidade,
        PUNIT as preco_unitario,
        CUSTOFINEST as custo_financeiro, 
        CUSTOFIN as custo_financeiro_movimento, 
        ST as valor_st, 
        VLIPI as valor_ipi, 
        --VLICMSST as valor_icms_st,
        VLDESCONTO as valor_desconto,
        case 
            when CODOPER in ('S', 'SD') then 'SAIDA'
            when CODOPER in ('E', 'ED') then 'ENTRADA'
            else 'OUTRO'
        end as tipo_movimento
    from source
    where DTCANCEL is null
)

select * from renamed