SELECT
    listing_id::BIGINT                                                  AS listing_id,
    date::DATE                                                          AS calendar_date,
    available = 't'                                                     AS available,
    NULLIF(REPLACE(REPLACE(price, '$', ''), ',', ''), '')::NUMERIC      AS price,
    NULLIF(REPLACE(REPLACE(adjusted_price, '$', ''), ',', ''), '')::NUMERIC
                                                                        AS adjusted_price,
    NULLIF(minimum_nights, '')::INT                                     AS minimum_nights,
    NULLIF(maximum_nights, '')::INT                                     AS maximum_nights
FROM {{ source('bronze', 'calendar_raw') }}
WHERE listing_id IS NOT NULL
  AND date IS NOT NULL
  AND date != ''
