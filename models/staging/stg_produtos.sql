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
        --CODCATEGORIA as id_categoria,
        CODMARCA as id_marca,
        --CUSTOULTENT as custo_ultima_entrada,
        CUSTOREP as custo_reposicao,
        --PRECOVENDA as preco_venda,
        --MARKUP as markup_produto,
        QTUNITCX as quantidade_por_caixa,
        UNIDADE as unidade_medida
        --cast(DTULTENT as date) as data_ultima_entrada
    from source
)

select * from renamed