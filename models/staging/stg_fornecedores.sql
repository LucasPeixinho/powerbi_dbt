with source as(
    select * from {{ source('cedep', 'pcfornec') }} 
),

renamed as(
    select
        CODFORNEC as id_fornecedor,
        FORNECEDOR as nome_fornecedor,
        REVENDA as finalidade_fornecedor,
        TIPOFORNEC as tipo_fornecedor,
        ESTADO as uf_fornecedor,
        CIDADE as cidade_fornecedor,
        BLOQUEIO as status_bloqueio,
        DTCADASTRO as data_cadastro
    from source
)

select * from renamed