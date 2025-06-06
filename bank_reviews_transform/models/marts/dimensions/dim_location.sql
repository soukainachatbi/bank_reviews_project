{{ config(materialized='table') }}

WITH location_stats AS (
    SELECT 
        location,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        COUNT(DISTINCT agency) as unique_agencies,
        (
            SELECT COUNT(DISTINCT bank) 
            FROM {{ ref('stg_reviews') }} sr2 
            WHERE sr2.location = sr.location
        ) as unique_banks,
        MIN(review_date) as first_review_date,
        MAX(review_date) as last_review_date
    FROM {{ ref('stg_reviews') }} sr
    WHERE location IS NOT NULL
    GROUP BY location
)

SELECT 
    ROW_NUMBER() OVER () AS location_key,
    location,
    total_reviews,
    ROUND(avg_rating::numeric, 2) as avg_rating,
    unique_agencies,
    unique_banks,
    first_review_date,
    last_review_date,
    CURRENT_TIMESTAMP as created_at
FROM location_stats
