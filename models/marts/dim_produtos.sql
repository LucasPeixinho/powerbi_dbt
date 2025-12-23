with produtos as (
    select * from {{ ref('int_produtos') }}
),

produtos_final as (
    select
        id_produto,
        descricao_produto,
        id_fornecedor,
        id_departamento,
        nome_departamento,
        id_secao,
        nome_secao,
        id_marca,
        nome_marca,
        custo_reposicao,
        quantidade_por_caixa,
        unidade_medida
    from produtos
)

select * from produtos_final