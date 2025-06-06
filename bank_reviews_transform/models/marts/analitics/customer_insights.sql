-- VUE : customer_insights (Insights temporels globaux)
{{ config(materialized='view', tags=['analytics', 'looker']) }}

SELECT 
    DATE_TRUNC('quarter', TO_DATE(fr.review_date, 'YYYY-MM-DD')) AS review_quarter,
    db.bank_name,
    COUNT(fr.review_id) AS total_reviews,
    COUNT(DISTINCT br.branch_key) AS active_branches,
    COUNT(DISTINCT loc.location_key) AS locations_covered,
    ROUND(AVG(fr.review_rating)::numeric, 2) AS avg_rating,
    ROUND(AVG(fr.vader_sentiment_score)::numeric, 3) AS avg_vader_sentiment,
    ROUND(AVG(fr.textblob_sentiment_score)::numeric, 3) AS avg_textblob_sentiment,
    ROUND(AVG(fr.sentiment_confidence)::numeric, 3) AS avg_confidence,
    SUM(fr.is_positive) AS positive_count,
    SUM(fr.is_negative) AS negative_count,
    SUM(fr.is_neutral) AS neutral_count,
    ROUND(SUM(fr.is_positive) * 100.0 / COUNT(fr.review_id), 2) AS positive_rate,
    ROUND(SUM(fr.is_negative) * 100.0 / COUNT(fr.review_id), 2) AS negative_rate,
    COUNT(*) FILTER (WHERE fr.review_rating = 5) AS rating_5_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 4) AS rating_4_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 3) AS rating_3_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 2) AS rating_2_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 1) AS rating_1_count,
    ROUND(
        (COUNT(*) FILTER (WHERE fr.review_rating >= 4) -
         COUNT(*) FILTER (WHERE fr.review_rating <= 2)) * 100.0 / COUNT(fr.review_id), 2
    ) AS nps_score,
    ROUND(AVG(fr.data_quality_score)::numeric, 2) AS avg_quality_score,
    ROUND(AVG(fr.text_length)::numeric, 1) AS avg_text_length,
    SUM(fr.is_detailed_review) AS detailed_reviews,
    SUM(fr.needs_urgent_action) AS urgent_reviews,
    LAG(ROUND(AVG(fr.review_rating)::numeric, 2)) OVER (
        PARTITION BY db.bank_name
        ORDER BY DATE_TRUNC('quarter', TO_DATE(fr.review_date, 'YYYY-MM-DD'))
    ) AS prev_quarter_rating,
    LAG(COUNT(fr.review_id)) OVER (
        PARTITION BY db.bank_name
        ORDER BY DATE_TRUNC('quarter', TO_DATE(fr.review_date, 'YYYY-MM-DD'))
    ) AS prev_quarter_volume
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_bank') }} db ON fr.bank_key = db.bank_key
JOIN {{ ref('dim_branch') }} br ON fr.branch_key = br.branch_key
JOIN {{ ref('dim_location') }} loc ON fr.location_key::TEXT = loc.location_key::TEXT
WHERE fr.review_date IS NOT NULL
  AND fr.review_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
  AND TO_DATE(fr.review_date, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '24 months'
GROUP BY review_quarter, db.bank_name
ORDER BY review_quarter DESC, db.bank_name