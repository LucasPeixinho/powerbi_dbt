with source as (
    select * from {{ source('cedep', 'pcnfent') }}
),

renamed as (
    select
        NUMTRANSENT as id_transacao_entrada,
        NUMNOTA as numero_nota,
        CODFORNEC as id_fornecedor,
        CODFILIAL as id_filial,
        DTENT as data_entrada,
        DTEMISSAO as data_emissao,
        VLTOTAL as valor_total_nota,
        VLFRETE as valor_frete,
        --VLOUTRASDESP as valor_outras_despesas,
        VLDESCONTO as valor_desconto
    from source
    where DTCANCEL is null
)

select * from renamed
