SELECT
    listing_id::BIGINT   AS listing_id,
    id::BIGINT           AS review_id,
    date::DATE           AS review_date,
    reviewer_id::BIGINT  AS reviewer_id,
    reviewer_name
FROM {{ source('bronze', 'reviews_raw') }}
WHERE listing_id IS NOT NULL
  AND id IS NOT NULL
  AND date IS NOT NULL
  AND date != ''
