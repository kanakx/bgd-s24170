-- silver.listings: cleaned and typed listing data
DROP TABLE IF EXISTS silver.listings CASCADE;
CREATE TABLE silver.listings AS
SELECT
    id::BIGINT                                          AS listing_id,
    host_id::BIGINT                                     AS host_id,
    host_name,
    NULLIF(host_since, '')::DATE                         AS host_since,
    NULLIF(host_response_time, 'N/A')                   AS host_response_time,
    NULLIF(REPLACE(NULLIF(host_response_rate, 'N/A'), '%', ''), '')::NUMERIC
                                                        AS host_response_rate,
    NULLIF(REPLACE(NULLIF(host_acceptance_rate, 'N/A'), '%', ''), '')::NUMERIC
                                                        AS host_acceptance_rate,
    CASE WHEN host_is_superhost = 't' THEN TRUE ELSE FALSE END
                                                        AS host_is_superhost,
    neighbourhood_cleansed                              AS neighbourhood,
    neighbourhood_group_cleansed                        AS neighbourhood_group,
    latitude::NUMERIC                                   AS latitude,
    longitude::NUMERIC                                  AS longitude,
    property_type,
    room_type,
    accommodates::INT                                   AS accommodates,
    NULLIF(bedrooms, '')::INT                           AS bedrooms,
    NULLIF(beds, '')::INT                               AS beds,
    NULLIF(REPLACE(REPLACE(price, '$', ''), ',', ''), '')::NUMERIC
                                                        AS price,
    minimum_nights::INT                                 AS minimum_nights,
    maximum_nights::INT                                 AS maximum_nights,
    number_of_reviews::INT                              AS number_of_reviews,
    NULLIF(first_review, '')::DATE                      AS first_review,
    NULLIF(last_review, '')::DATE                       AS last_review,
    NULLIF(review_scores_rating, '')::NUMERIC           AS review_scores_rating,
    NULLIF(review_scores_accuracy, '')::NUMERIC         AS review_scores_accuracy,
    NULLIF(review_scores_cleanliness, '')::NUMERIC      AS review_scores_cleanliness,
    NULLIF(review_scores_checkin, '')::NUMERIC          AS review_scores_checkin,
    NULLIF(review_scores_communication, '')::NUMERIC    AS review_scores_communication,
    NULLIF(review_scores_location, '')::NUMERIC         AS review_scores_location,
    NULLIF(review_scores_value, '')::NUMERIC            AS review_scores_value,
    CASE WHEN instant_bookable = 't' THEN TRUE ELSE FALSE END
                                                        AS instant_bookable,
    NULLIF(reviews_per_month, '')::NUMERIC              AS reviews_per_month
FROM bronze.listings_raw
WHERE id IS NOT NULL AND id != '';

-- silver.reviews: cleaned review data
DROP TABLE IF EXISTS silver.reviews CASCADE;
CREATE TABLE silver.reviews AS
SELECT
    listing_id::BIGINT   AS listing_id,
    id::BIGINT           AS review_id,
    date::DATE           AS review_date,
    reviewer_id::BIGINT  AS reviewer_id,
    reviewer_name
FROM bronze.reviews_raw
WHERE listing_id IS NOT NULL
  AND id IS NOT NULL
  AND date IS NOT NULL AND date != '';

-- silver.calendar: cleaned calendar data
DROP TABLE IF EXISTS silver.calendar CASCADE;
CREATE TABLE silver.calendar AS
SELECT
    listing_id::BIGINT  AS listing_id,
    date::DATE          AS calendar_date,
    available = 't'     AS available,
    NULLIF(REPLACE(REPLACE(price, '$', ''), ',', ''), '')::NUMERIC
                        AS price,
    NULLIF(REPLACE(REPLACE(adjusted_price, '$', ''), ',', ''), '')::NUMERIC
                        AS adjusted_price,
    NULLIF(minimum_nights, '')::INT AS minimum_nights,
    NULLIF(maximum_nights, '')::INT AS maximum_nights
FROM bronze.calendar_raw
WHERE listing_id IS NOT NULL AND date IS NOT NULL AND date != '';
