{{ config(materialized='table') }}

WITH sentiment_joined AS (
    SELECT 
        r.*,
        s.sentiment,
        s.vader_compound,
        s.textblob_polarity,
        s.confidence
    FROM {{ ref('stg_reviews') }} r
    LEFT JOIN {{ source('public', 'sentiment_analysis') }} s
        ON r.review_id = s.review_id
),

final AS (
    SELECT 
        *,
        -- Classification des notes
        CASE 
            WHEN rating >= 4 THEN 'positive'
            WHEN rating <= 2 THEN 'negative'
            ELSE 'neutral'
        END as rating_sentiment,
        
        -- Cohérence sentiment/note
        CASE 
            WHEN sentiment = 'positive' AND rating >= 4 THEN 'coherent'
            WHEN sentiment = 'negative' AND rating <= 2 THEN 'coherent'
            WHEN sentiment = 'neutral' AND rating = 3 THEN 'coherent'
            ELSE 'incoherent'
        END as sentiment_rating_consistency,
        
        -- Catégorisation de l'agence
        CASE 
            WHEN agency LIKE '%Centre%' OR agency LIKE '%centre%' THEN 'centre-ville'
            WHEN agency LIKE '%Mall%' OR agency LIKE '%Centre Commercial%' THEN 'centre-commercial'
            ELSE 'standard'
        END as agency_type
    FROM sentiment_joined
)

SELECT * FROM final