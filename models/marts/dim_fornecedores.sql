with fornecedores as (
    select * from {{ ref('int_fornecedores') }}
),

renamed as (
    select
        id_fornecedor,
        nome_fornecedor,
        finalidade_fornecedor,
        tipo_fornecedor,
        uf_fornecedor,
        cidade_fornecedor,
        status_bloqueio,
        data_cadastro
    from fornecedores
)

select * from renamed