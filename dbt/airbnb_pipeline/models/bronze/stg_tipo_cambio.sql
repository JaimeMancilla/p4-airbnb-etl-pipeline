{{ config(materialized='view') }}

SELECT *
FROM {{ source('raw', 'tipo_cambio') }}