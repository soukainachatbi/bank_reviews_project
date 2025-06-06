{{ config(materialized='table') }}

WITH branch_stats AS (
    SELECT 
        agency,
        bank,
        location,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        MIN(review_date) as first_review_date,
        MAX(review_date) as last_review_date
    FROM {{ ref('stg_reviews') }}
    WHERE agency IS NOT NULL
    GROUP BY agency, bank, location
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['agency', 'bank', 'location']) }} AS branch_key,
    agency,
    bank,
    location,
    total_reviews,
    ROUND(avg_rating::numeric, 2) as avg_rating,
    first_review_date,
    last_review_date,
    CURRENT_TIMESTAMP as created_at
FROM branch_stats
