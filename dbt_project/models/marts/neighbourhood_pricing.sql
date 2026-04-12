SELECT
    neighbourhood,
    neighbourhood_group,
    room_type,
    COUNT(*)                                                            AS listing_count,
    ROUND(AVG(price), 2)                                                AS avg_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::NUMERIC, 2)
                                                                        AS median_price,
    ROUND(AVG(review_scores_rating), 2)                                 AS avg_rating,
    ROUND(AVG(reviews_per_month), 2)                                    AS avg_reviews_per_month
FROM {{ ref('stg_listings') }}
WHERE price IS NOT NULL AND price > 0
GROUP BY neighbourhood, neighbourhood_group, room_type
ORDER BY neighbourhood_group, neighbourhood, room_type
