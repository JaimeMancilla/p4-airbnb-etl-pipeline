{{ config(materialized='table') }}

SELECT
    CAST(fecha AS DATE) AS fecha,
    usd_oficial_compra,	
    usd_oficial_venta,	
    usd_blue_compra,
    usd_blue_venta
    
FROM {{ ref('stg_tipo_cambio') }}