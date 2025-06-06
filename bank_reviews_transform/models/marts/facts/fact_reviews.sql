{{ config(materialized='table') }}

WITH fact_base AS (
    SELECT 
        -- Clé primaire
        review_id,
        
        -- Clés étrangères (dimensions)
        {{ dbt_utils.generate_surrogate_key(['bank']) }} as bank_key,
        {{ dbt_utils.generate_surrogate_key(['bank', 'agency']) }} as branch_key,
        {{ dbt_utils.generate_surrogate_key(['location']) }} as location_key,
        {{ dbt_utils.generate_surrogate_key(['sentiment', 'feedback_category', 'business_segment']) }} as sentiment_key,
        
        -- Attributs dégénérés (informations qui restent au niveau du fait)
        author,
        review_date,
        detected_language,
        review_quality,
        sentiment_rating_consistency,

        -- Topics pour l'analyse
        topic_category,
        -- dominant_topic, -- décommente si tu veux aussi ce champ

        -- Mesures numériques
        rating as review_rating,
        text_length,
        vader_compound as vader_sentiment_score,
        textblob_polarity as textblob_sentiment_score,
        confidence as sentiment_confidence,
        COALESCE(topic_confidence, 0) as topic_confidence_score,
        
        -- Mesures calculées
        CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END as is_positive,
        CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END as is_negative,
        CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END as is_neutral,
        CASE WHEN rating >= 4 THEN 1 ELSE 0 END as is_high_rating,
        CASE WHEN rating <= 2 THEN 1 ELSE 0 END as is_low_rating,
        CASE WHEN sentiment_rating_consistency = 'coherent' THEN 1 ELSE 0 END as is_coherent,
        CASE WHEN action_priority = 'priorite_haute' THEN 1 ELSE 0 END as needs_urgent_action,
        
        -- Timestamp de traitement
        processed_at,
        CURRENT_TIMESTAMP as fact_created_at
        
    FROM {{ ref('reviews_enriched') }}
    WHERE review_id IS NOT NULL
),

-- Ajout de métriques de qualité des données
fact_enhanced AS (
    SELECT 
        *,
        -- Score de qualité composite
        CASE 
            WHEN sentiment_confidence > 0.6 AND topic_confidence_score > 0.3 AND text_length > 50 THEN 5
            WHEN sentiment_confidence > 0.4 AND topic_confidence_score > 0.2 AND text_length > 30 THEN 4
            WHEN sentiment_confidence > 0.3 AND text_length > 20 THEN 3
            WHEN sentiment_confidence > 0.2 AND text_length > 10 THEN 2
            ELSE 1
        END as data_quality_score,
        
        -- Indicateurs pour les KPIs
        CASE WHEN ABS(vader_sentiment_score) > 0.5 THEN 1 ELSE 0 END as is_strong_sentiment,
        CASE WHEN text_length > 100 THEN 1 ELSE 0 END as is_detailed_review
        
    FROM fact_base
)

SELECT * FROM fact_enhanced