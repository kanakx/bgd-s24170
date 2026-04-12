SELECT
    l.host_id,
    l.host_name,
    l.host_is_superhost,
    l.host_response_rate,
    COUNT(DISTINCT l.listing_id)                AS total_listings,
    SUM(l.number_of_reviews)                    AS total_reviews,
    ROUND(AVG(l.review_scores_rating), 2)       AS avg_rating,
    ROUND(AVG(l.price), 2)                      AS avg_listing_price,
    COUNT(DISTINCT r.review_id)                 AS review_count_verified
FROM {{ ref('stg_listings') }} l
LEFT JOIN {{ ref('stg_reviews') }} r ON l.listing_id = r.listing_id
GROUP BY l.host_id, l.host_name, l.host_is_superhost, l.host_response_rate
ORDER BY total_reviews DESC
