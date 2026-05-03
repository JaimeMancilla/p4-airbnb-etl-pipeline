{{ config(materialized='table') }}

SELECT
    CAST(fecha AS DATE) AS fecha,
    temp_max,
    temp_min,
    precipitacion
    
FROM {{ ref('stg_clima') }}
