-- VUE : branch_performance (Performance des agences)
{{ config(materialized='view', tags=['analytics', 'looker']) }}

SELECT 
    db.bank_name,
    br.agency AS branch_name,
    loc.location AS location_name,
    COUNT(fr.review_id) AS total_reviews,
    ROUND(AVG(fr.review_rating)::numeric, 2) AS avg_rating,
    ROUND(AVG(fr.vader_sentiment_score)::numeric, 3) AS avg_vader_sentiment,
    ROUND(AVG(fr.textblob_sentiment_score)::numeric, 3) AS avg_textblob_sentiment,
    ROUND(AVG(fr.sentiment_confidence)::numeric, 3) AS avg_confidence,
    SUM(fr.is_positive) AS positive_reviews,
    SUM(fr.is_negative) AS negative_reviews,
    SUM(fr.is_neutral) AS neutral_reviews,
    ROUND(SUM(fr.is_positive) * 100.0 / COUNT(fr.review_id), 2) AS positive_percentage,
    ROUND(SUM(fr.is_negative) * 100.0 / COUNT(fr.review_id), 2) AS negative_percentage,
    ROUND(SUM(fr.is_neutral) * 100.0 / COUNT(fr.review_id), 2) AS neutral_percentage,
    ROUND(AVG(fr.data_quality_score)::numeric, 2) AS avg_quality_score,
    ROUND(AVG(fr.text_length)::numeric, 1) AS avg_text_length,
    SUM(fr.is_detailed_review) AS detailed_reviews_count,
    SUM(fr.is_strong_sentiment) AS strong_sentiment_count,
    SUM(fr.needs_urgent_action) AS urgent_action_needed,
    COUNT(*) FILTER (WHERE fr.is_coherent = 0) AS incoherent_reviews,
    COUNT(*) FILTER (WHERE fr.review_rating = 5) AS rating_5_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 4) AS rating_4_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 3) AS rating_3_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 2) AS rating_2_count,
    COUNT(*) FILTER (WHERE fr.review_rating = 1) AS rating_1_count,
    RANK() OVER (ORDER BY AVG(fr.review_rating) DESC) AS rating_rank,
    RANK() OVER (ORDER BY AVG(fr.vader_sentiment_score) DESC) AS sentiment_rank,
    RANK() OVER (ORDER BY COUNT(fr.review_id) DESC) AS volume_rank,
    MIN(TO_DATE(fr.review_date, 'YYYY-MM-DD')) AS first_review_date,
    MAX(TO_DATE(fr.review_date, 'YYYY-MM-DD')) AS last_review_date
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_bank') }} db ON fr.bank_key = db.bank_key
JOIN {{ ref('dim_branch') }} br ON fr.branch_key = br.branch_key
JOIN {{ ref('dim_location') }} loc ON fr.location_key::TEXT = loc.location_key::TEXT
WHERE fr.review_date IS NOT NULL
  AND fr.review_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
GROUP BY db.bank_name, br.agency, loc.location
HAVING COUNT(fr.review_id) >= 3
ORDER BY avg_rating DESC, total_reviews DESC