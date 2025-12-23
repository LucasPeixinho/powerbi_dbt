with source as (
    select * from {{source('cedep', 'pcfilial') }}
),

renamed as (
    select
        CODIGO as id_filial, 
        RAZAOSOCIAL as razao_social_filial,
        ENDERECO as endereco_filial,
        CIDADE as cidade_filial
    from source
    where CODIGO <> '99'
)

select * from renamed