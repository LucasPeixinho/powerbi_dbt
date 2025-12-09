{% macro filtro_periodo(coluna_data, var_inicio='dt_inicio', var_fim='dt_fim', default_inicio='2018-01-01', usar_data_atual=true) %}
    {%- set data_fim = var(var_fim, modules.datetime.date.today().strftime('%Y-%m-%d') if usar_data_atual else '2025-12-31') -%}
    {{ coluna_data }} BETWEEN TO_DATE('{{ var(var_inicio, default_inicio) }}', 'YYYY-MM-DD') 
                          AND TO_DATE('{{ data_fim }}', 'YYYY-MM-DD')
{% endmacro %}