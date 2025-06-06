{{ config(
    materialized='view',
    tags=['analytics', 'looker']
) }}

SELECT 
    db.bank_name,
    br.agency AS branch_name,
    loc.location AS location_name,
    se.sentiment AS sentiment_label,
    DATE_TRUNC('month', TO_DATE(fr.review_date, 'YYYY-MM-DD')) AS review_month,
    COUNT(*) AS review_count,
    ROUND(AVG(fr.review_rating)::numeric, 2) AS avg_rating,
    ROUND(AVG(fr.vader_sentiment_score)::numeric, 3) AS avg_vader_score,
    ROUND(AVG(fr.textblob_sentiment_score)::numeric, 3) AS avg_textblob_score,
    ROUND(AVG(fr.sentiment_confidence)::numeric, 3) AS avg_confidence,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
        PARTITION BY db.bank_name, DATE_TRUNC('month', TO_DATE(fr.review_date, 'YYYY-MM-DD'))
    ), 2) AS sentiment_percentage,
    ROUND(AVG(fr.data_quality_score)::numeric, 2) AS avg_quality_score,
    COUNT(*) FILTER (WHERE fr.is_coherent = 1) AS coherent_reviews,
    COUNT(*) FILTER (WHERE fr.needs_urgent_action = 1) AS urgent_reviews
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_bank') }} db ON fr.bank_key = db.bank_key
JOIN {{ ref('dim_branch') }} br ON fr.branch_key = br.branch_key
JOIN {{ ref('dim_location') }} loc ON fr.location_key::TEXT = loc.location_key::TEXT
JOIN {{ ref('dim_sentiment') }} se ON fr.sentiment_key::TEXT = se.sentiment_key::TEXT
WHERE fr.review_date IS NOT NULL
  AND fr.review_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
  AND TO_DATE(fr.review_date, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 
    db.bank_name, br.agency, loc.location, se.sentiment,
    DATE_TRUNC('month', TO_DATE(fr.review_date, 'YYYY-MM-DD'))
ORDER BY 
    review_month DESC, db.bank_name, sentiment_percentage DESC
