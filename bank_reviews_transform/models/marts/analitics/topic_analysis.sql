-- VUE : topic_analysis (Analyse des topics)
{{ config(materialized='view', tags=['analytics', 'looker']) }}

SELECT
    db.bank_name,
    dbr.agency AS branch_name,
    dl.location AS location_name,
    fr.topic_category AS topic,
    fr.topic_confidence_score,
    COUNT(fr.review_id) AS topic_occurrence,
    ROUND(AVG(fr.review_rating)::numeric, 2) AS avg_rating,
    ROUND(AVG(fr.vader_sentiment_score)::numeric, 3) AS avg_vader_score,
    ROUND(AVG(fr.textblob_sentiment_score)::numeric, 3) AS avg_textblob_score,
    ROUND(AVG(fr.sentiment_confidence)::numeric, 3) AS avg_confidence,
    ROUND(AVG(fr.text_length)::numeric, 1) AS avg_text_length,
    SUM(fr.needs_urgent_action) AS urgent_reviews,
    MIN(CASE WHEN fr.review_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_DATE(fr.review_date, 'YYYY-MM-DD') END) AS first_review_date,
    MAX(CASE WHEN fr.review_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_DATE(fr.review_date, 'YYYY-MM-DD') END) AS last_review_date,
    COUNT(*) FILTER (WHERE fr.is_coherent = 1) AS coherent_reviews,
    COUNT(*) FILTER (WHERE fr.is_coherent = 0) AS incoherent_reviews
FROM {{ ref('fact_reviews') }} fr
LEFT JOIN {{ ref('dim_bank') }} db ON fr.bank_key = db.bank_key
LEFT JOIN {{ ref('dim_branch') }} dbr ON fr.branch_key = dbr.branch_key
LEFT JOIN {{ ref('dim_location') }} dl ON fr.location_key::TEXT = dl.location_key::TEXT
WHERE fr.topic_confidence_score >= 0.6
  AND fr.review_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
  AND fr.topic_category IS NOT NULL
GROUP BY
    db.bank_name,
    dbr.agency,
    dl.location,
    fr.topic_category,
    fr.topic_confidence_score
ORDER BY
    topic_occurrence DESC,
    avg_rating DESC
