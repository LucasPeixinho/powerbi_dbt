with fornecedores as (
    select * from {{ ref('stg_fornecedores') }}
),

renamed as (
    select
        id_fornecedor,
        nome_fornecedor,
        case finalidade_fornecedor
            when 'T' then 'Transportadora'
            when 'B' then 'Beneficiamento'
            when 'C' then 'Comunicação'
            when 'O' then 'Consumo'
            when 'S' then 'Revenda'
            when 'E' then 'Energia'
            when 'X' then 'Exportador'
            when 'N' then 'Não Revenda'
            when 'P' then 'Profissional liberal'
            ELSE 'Desconhecido'
        end as finalidade_fornecedor,
        case tipo_fornecedor
            when 'D' then 'Central de distribuição'
            when 'C' then 'Comércio atacadista'
            when 'V' then 'Varejista'
            when 'F' then 'Filial'
            when 'I' then 'Indústria'
            when 'O' then 'Outras'
        end as tipo_fornecedor,
        uf_fornecedor,
        cidade_fornecedor,
        case when status_bloqueio = 'S' then 'Bloqueado' else 'Ativo' end as status_bloqueio,
        data_cadastro
    from fornecedores
)

select * from renamed