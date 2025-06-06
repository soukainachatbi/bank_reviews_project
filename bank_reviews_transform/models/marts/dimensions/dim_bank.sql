{{ config(materialized='table') }}

WITH bank_data AS (
    SELECT DISTINCT
        bank,
        COUNT(*) OVER (PARTITION BY bank) as total_reviews,
        ROUND(AVG(rating) OVER (PARTITION BY bank), 2) as avg_rating,
        COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) OVER (PARTITION BY bank) as positive_reviews,
        COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) OVER (PARTITION BY bank) as negative_reviews,
        COUNT(CASE WHEN sentiment = 'neutral' THEN 1 END) OVER (PARTITION BY bank) as neutral_reviews
    FROM {{ ref('fact_reviews') }}
    WHERE bank IS NOT NULL
),

bank_classification AS (
    SELECT 
        *,
        CASE 
            WHEN avg_rating >= 4.0 THEN 'Excellent'
            WHEN avg_rating >= 3.5 THEN 'Bon'
            WHEN avg_rating >= 3.0 THEN 'Moyen'
            ELSE 'Faible'
        END as performance_category,
        
        CASE 
            WHEN total_reviews >= 100 THEN 'Grande banque'
            WHEN total_reviews >= 50 THEN 'Banque moyenne'
            ELSE 'Petite banque'
        END as size_category,
        
        ROUND((positive_reviews * 100.0 / total_reviews), 1) as positive_percentage
    FROM bank_data
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['bank']) }} as bank_key,
    bank as bank_name,
    total_reviews,
    avg_rating,
    positive_reviews,
    negative_reviews,
    neutral_reviews,
    positive_percentage,
    performance_category,
    size_category,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM bank_classification