with source as (
    select * from {{ source('cedep', 'pcprodut') }}
),

renamed as (
    select
        CODPROD as id_produto,
        CODFORNEC as id_fornecedor,
        DESCRICAO as descricao_produto,
        CODEPTO as id_departamento,
        CODSEC as id_secao,
        CODMARCA as id_marca,
        CUSTOREP as custo_reposicao,
        QTUNITCX as quantidade_por_caixa,
        UNIDADE as unidade_medida
    from source
)

select * from renamed