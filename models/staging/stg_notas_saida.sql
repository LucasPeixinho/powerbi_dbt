with source as (
    select * from {{ source('cedep', 'pcnfsaid') }}
),

renamed as (
    select
        NUMTRANSVENDA as id_transacao_venda,
        NUMNOTA as numero_nota,
        CODCLI as id_cliente,
        CODFILIAL as id_filial,
        CODUSUR as id_vendedor,
        CODSUPERVISOR as id_supervisor, 
        DTSAIDA as data_saida, 
        VLTOTAL as valor_total_nota, 
        VLTABELA as valor_tabela, 
        VLBONIFIC as valor_bonificacao, 
        VLFRETE as valor_frete, 
        VLOUTRASDESP as valor_outras_despesas, 
        CONDVENDA as condicao_venad
    from source
    where DTCANCEL is null
)

select * from renamed