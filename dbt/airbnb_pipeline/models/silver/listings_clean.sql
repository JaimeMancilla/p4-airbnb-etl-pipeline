{{ config(materialized='table') }}

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
    CAST(first_review AS DATE) AS first_review,
    CAST(last_review AS DATE)  AS last_review,
    CAST(last_scraped AS DATE) AS last_scraped
FROM {{ ref('stg_listings') }}
WHERE id IS NOT NULL