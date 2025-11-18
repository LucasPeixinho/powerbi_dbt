{{ config(
    materialized='table'
) }}

WITH datas_ref AS (
    SELECT
        ultimo_dia_util_mes AS data_ref,
        ref_10d,
        ref_6m
    FROM {{ ref('stg_ultimo_dia_util') }}
),

inad AS (
    SELECT
        d.data_ref,
        s.CODCLI,
        s.CODFILIAL,
        s.CODUSUR,
        s.CODCOB,
        SUM(s.VALOR) AS valor_inadimplente
    FROM datas_ref d
    JOIN {{ ref('stg_inadimplencia') }} s
        ON s.DTVENC BETWEEN d.ref_6m AND d.ref_10d
        AND (s.DTPAG IS NULL OR s.DTPAG > d.data_ref)
    GROUP BY
        d.data_ref,
        s.CODCLI,
        s.CODFILIAL,
        s.CODUSUR,
        s.CODCOB
)

SELECT *
FROM inad
