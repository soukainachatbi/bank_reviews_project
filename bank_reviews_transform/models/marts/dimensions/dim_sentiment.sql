{{ config(materialized='table') }}

WITH sentiment_stats AS (
    SELECT 
        bank,
        sentiment,
        COUNT(*) AS total_reviews,
        AVG(rating) AS avg_rating,
        AVG(vader_compound::numeric) AS avg_vader_compound,
        MIN(review_date) AS first_review_date,
        MAX(review_date) AS last_review_date
    FROM {{ ref('reviews_enriched') }}
    WHERE sentiment IS NOT NULL
    GROUP BY bank, sentiment
),

with_keys AS (
    SELECT 
        ROW_NUMBER() OVER () AS sentiment_key,
        *
    FROM sentiment_stats
)

SELECT
    sentiment_key,
    bank,
    sentiment,
    ROUND(avg_vader_compound, 3) AS avg_vader_compound,
    ROUND(avg_rating, 2) AS avg_rating,
    total_reviews,
    first_review_date,
    last_review_date,
    CURRENT_TIMESTAMP AS created_at
FROM with_keys
