-- Bronze tables are dynamically created by src/load.py using pandas to_sql().
-- This file documents the expected schema for reference.
-- All columns are loaded as TEXT in the bronze layer.

-- bronze.listings_raw: ~80 columns from listings.csv
-- Key columns: id, host_id, host_name, neighbourhood_cleansed, room_type,
--              price, number_of_reviews, review_scores_*, availability_*

-- bronze.reviews_raw: listing_id, id, date, reviewer_id, reviewer_name, comments

-- bronze.calendar_raw: listing_id, date, available, price, adjusted_price,
--                      minimum_nights, maximum_nights
