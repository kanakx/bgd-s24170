-- gold.neighbourhood_pricing: aggregated pricing stats by neighbourhood and room type
DROP TABLE IF EXISTS gold.neighbourhood_pricing CASCADE;
CREATE TABLE gold.neighbourhood_pricing AS
SELECT
    neighbourhood,
    neighbourhood_group,
    room_type,
    COUNT(*)                                AS listing_count,
    ROUND(AVG(price), 2)                    AS avg_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::NUMERIC, 2)
                                            AS median_price,
    ROUND(AVG(review_scores_rating), 2)     AS avg_rating,
    ROUND(AVG(reviews_per_month), 2)        AS avg_reviews_per_month
FROM silver.listings
WHERE price IS NOT NULL AND price > 0
GROUP BY neighbourhood, neighbourhood_group, room_type
ORDER BY neighbourhood_group, neighbourhood, room_type;

-- gold.host_performance: host-level metrics joined with review counts
DROP TABLE IF EXISTS gold.host_performance CASCADE;
CREATE TABLE gold.host_performance AS
SELECT
    l.host_id,
    l.host_name,
    l.host_is_superhost,
    l.host_response_rate,
    COUNT(DISTINCT l.listing_id)    AS total_listings,
    SUM(l.number_of_reviews)        AS total_reviews,
    ROUND(AVG(l.review_scores_rating), 2)
                                    AS avg_rating,
    ROUND(AVG(l.price), 2)         AS avg_listing_price,
    COUNT(DISTINCT r.review_id)     AS review_count_verified
FROM silver.listings l
LEFT JOIN silver.reviews r ON l.listing_id = r.listing_id
GROUP BY l.host_id, l.host_name, l.host_is_superhost, l.host_response_rate
ORDER BY total_reviews DESC;

-- gold.monthly_availability: monthly availability rate by neighbourhood
DROP TABLE IF EXISTS gold.monthly_availability CASCADE;
CREATE TABLE gold.monthly_availability AS
SELECT
    l.neighbourhood,
    l.neighbourhood_group,
    DATE_TRUNC('month', c.calendar_date)::DATE  AS month,
    COUNT(*)                                     AS total_days,
    SUM(CASE WHEN c.available THEN 1 ELSE 0 END) AS available_days,
    ROUND(
        100.0 * SUM(CASE WHEN c.available THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                            AS availability_pct,
    ROUND(AVG(c.price), 2)                       AS avg_daily_price
FROM silver.calendar c
JOIN silver.listings l ON c.listing_id = l.listing_id
GROUP BY l.neighbourhood, l.neighbourhood_group, DATE_TRUNC('month', c.calendar_date)
ORDER BY month, l.neighbourhood_group, l.neighbourhood;
