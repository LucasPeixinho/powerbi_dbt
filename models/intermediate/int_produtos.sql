with produtos as (
    select * from {{ ref('stg_produtos') }}
),

departamentos as (
    select * from {{ ref('stg_departamento') }}
),

secoes as (
    select * from {{ ref('stg_secao') }}
),

marcas as (
    select * from {{ ref('stg_marca') }}
),

produto_completo as (
    select
        p.id_produto,
        p.descricao_produto,
        p.id_fornecedor,
        p.id_departamento,
        d.nome_departamento,
        p.id_secao,
        s.nome_secao,
        p.id_marca,
        m.nome_marca,
        p.custo_reposicao,
        p.quantidade_por_caixa,
        p.unidade_medida
    from produtos p
    left join departamentos d on p.id_departamento = d.id_departamento
    left join secoes s on p.id_secao = s.id_secao
    left join marcas m on p.id_marca = m.id_marca
)

select * from produto_completo