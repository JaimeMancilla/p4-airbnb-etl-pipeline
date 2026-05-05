{{ config(materialized='table') }}

WITH tipo_cambio_enero AS (
    SELECT
        ROUND(AVG(usd_oficial_venta)::numeric, 2) AS usd_oficial_venta_prom,
        ROUND(AVG(usd_blue_venta)::numeric, 2)    AS usd_blue_venta_prom
    FROM {{ ref('tipo_cambio_clean') }}
    WHERE fecha BETWEEN '2026-01-01' AND '2026-01-31'
),

clima_enero AS (
    SELECT
        ROUND(AVG(temp_max)::numeric, 2)      AS temp_max_prom,
        ROUND(AVG(temp_min)::numeric, 2)      AS temp_min_prom,
        ROUND(AVG(precipitacion)::numeric, 2) AS precipitacion_prom
    FROM {{ ref('clima_clean') }}
    WHERE fecha BETWEEN '2026-01-01' AND '2026-01-31'
),

listings_enriquecido AS (
    SELECT
        id,
        neighbourhood_cleansed,
        room_type,
        accommodates,
        bathrooms,
        beds,
        review_scores_rating,
        reviews_per_month,
        tiene_resenas,
        antiguedad_total,
        first_review,
        last_review,
        last_scraped
    FROM {{ ref('listings_clean') }}
)

SELECT
    l.*,
    t.usd_oficial_venta_prom,
    t.usd_blue_venta_prom,
    c.temp_max_prom,
    c.temp_min_prom,
    c.precipitacion_prom
FROM listings_enriquecido l
CROSS JOIN tipo_cambio_enero t
CROSS JOIN clima_enero c