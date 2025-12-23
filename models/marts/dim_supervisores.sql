with supervisores as (
    select * from {{ ref('int_supervisores') }}
),

final as (
    select
        id_supervisor,
        nome_supervisor,
        supervisor
    from supervisores
)

select * from final
