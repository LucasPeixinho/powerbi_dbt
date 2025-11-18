{{ config(
    materialized='table'
) }}

WITH ultimos_por_mes AS (
    SELECT 
        TRUNC(data, 'MM') AS mes_ref,
        MAX(data) AS ultimo_dia_util_mes
    FROM {{ ref('stg_calendario') }}
    WHERE dia_util = 1
    GROUP BY TRUNC(data, 'MM')
),

datas_calculadas AS (
    SELECT
        ultimo_dia_util_mes,
        ultimo_dia_util_mes - 10 AS ref_10d,
        ADD_MONTHS(ultimo_dia_util_mes - 10, -6) AS ref_6m
    FROM ultimos_por_mes
)

SELECT 
    ultimo_dia_util_mes,
    ref_10d,
    ref_6m,
    EXTRACT(YEAR FROM ultimo_dia_util_mes) AS ano,
    EXTRACT(MONTH FROM ultimo_dia_util_mes) AS mes
FROM datas_calculadas
ORDER BY ultimo_dia_util_mes