{{ config(materialized='view') }}

WITH cleaned_reviews AS (
    SELECT 
        -- IDs et métadonnées
        ROW_NUMBER() OVER (ORDER BY bank, agency, author, review_date) as review_id,
        bank,
        agency,
        url,
        location,
        author,
        rating,
        review_date,
        review_text,
        
        -- Nettoyage du texte
        LOWER(TRIM(review_text)) as clean_text,
        
        -- Extraction de la langue (basique)
        CASE 
            WHEN review_text ~* '[àáâäèéêëìíîïòóôöùúûü]|ça|très|être|avoir' THEN 'fr'
            WHEN review_text ~* 'والله|الله|مع|في|على|هذا|هذه' THEN 'ar'
            WHEN review_text ~* '\bthe\b|\band\b|\bor\b|\bof\b|\bin\b|\bto\b' THEN 'en'
            ELSE 'unknown'
        END as detected_language,
        
        -- Longueur du texte
        LENGTH(review_text) as text_length,
        
        -- Indicateurs de qualité
        CASE 
            WHEN LENGTH(TRIM(review_text)) < 10 THEN 'short'
            WHEN LENGTH(TRIM(review_text)) > 500 THEN 'long'
            ELSE 'normal'
        END as text_quality,
        
        -- Timestamp de traitement
        CURRENT_TIMESTAMP as processed_at
        
    FROM {{ source('public', 'staging_reviews') }}
    WHERE 
        review_text IS NOT NULL 
        AND TRIM(review_text) != ''
        AND rating IS NOT NULL
        AND rating BETWEEN 1 AND 5
),

-- Suppression des doublons
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY bank, agency, author, clean_text 
            ORDER BY review_date DESC
        ) as rn
    FROM cleaned_reviews
)

SELECT *
FROM deduplicated
WHERE rn = 1