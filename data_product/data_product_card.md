# Data Product Card — Field Values

## 1. PRODUCT OVERVIEW

Product name: Boston Airbnb Neighbourhood Pricing Analytics
Owner / Team: Kacper Kanak (s24170)
Business domain: Short-term rental / Hospitality
Primary users: Data analysts, urban planners, hospitality researchers
Version / update date: v1.0 / 2026-05-10

Format: CSV / PostgreSQL Table
Refresh: Daily (Airflow-scheduled)
Access: GitHub Repository + Docker PostgreSQL
Quality metrics: Completeness 100%, Uniqueness 100%, Validity 100%
Status: Production-like


## 2. PURPOSE & VALUE

This data product provides a consolidated view of Airbnb pricing dynamics across Boston neighbourhoods, broken down by room type. It enables analysts to compare average and median nightly prices, listing density, and guest satisfaction ratings without needing to process raw listing data. Urban planners can identify high-demand areas and pricing trends, while researchers gain a ready-made dataset for studying short-term rental market segmentation. The product aggregates over 4,400 raw listings into a concise 55-row analytical table.


## 3. DATA SOURCES

Inside Airbnb — listings.csv.gz | File (HTTP) | Quarterly snapshot
Inside Airbnb — reviews.csv.gz  | File (HTTP) | Quarterly snapshot
Inside Airbnb — calendar.csv.gz | File (HTTP) | Quarterly snapshot


## 4. HOW TO ACCESS THIS PRODUCT

Where: GitHub repository (private, professor added as collaborator)
Direct link: github.com/kanakx/bgd-s24170 -> data_product/neighbourhood_pricing.csv
How to download: Download neighbourhood_pricing.csv from the repo, or run docker compose up -d postgres and query SELECT * FROM gold.neighbourhood_pricing; on localhost:5432/airbnb_nyc (user: airbnb, pass: airbnb_pass)
Credentials: DB credentials in .env.example; GitHub access granted to professor
Example table name: gold.neighbourhood_pricing


## 5. QUALITY SNAPSHOT

Completeness: 100% — all key columns (neighbourhood, room_type, avg_price, listing_count) fully populated
Uniqueness: 100% — every (neighbourhood, room_type) combination is unique
Validity: 100% — all avg_price values are positive (> $0)
Row count: 55 rows (25 neighbourhoods x 1-4 room types)
Freshness: Source snapshot Sep 2025 / Pipeline last run 2026-05-10

All metrics exceed the 99% threshold defined in data_product_contract.yaml.


## 6. HIGH-LEVEL FLOW

Inside Airbnb CSVs -> Kafka topics -> bronze.listings_raw -> silver.stg_listings (dbt) -> gold.neighbourhood_pricing (dbt) -> neighbourhood_pricing.csv


## 7. SCHEMA OVERVIEW

neighbourhood         | text    | Boston neighbourhood name                    | Back Bay
neighbourhood_group   | text    | Higher-level area grouping (empty for Boston) | —
room_type             | text    | Airbnb listing category                      | Entire home/apt
listing_count         | bigint  | Number of active listings in this group       | 273
avg_price             | numeric | Average nightly price (USD)                   | 311.26
median_price          | numeric | Median nightly price (USD)                    | 275.00
avg_rating            | numeric | Average guest review score (0-5)              | 4.69
avg_reviews_per_month | numeric | Average monthly review frequency              | 1.84


## 8. REFRESH & OPERATIONS

Refresh frequency: Daily (@daily Airflow schedule)
Update trigger: Apache Airflow DAG (airbnb_elt_pipeline)
Storage location: PostgreSQL (Docker) + CSV in Git
Expected batch size: 55 rows per full refresh (aggregated from 4,419 listings)
Dependencies: Inside Airbnb HTTP endpoint, Apache Kafka, PostgreSQL, dbt


## 9. EXAMPLE USAGE

Business question: Which Boston neighbourhoods have the highest median price for entire-home listings?

SELECT neighbourhood, median_price, avg_rating, listing_count
FROM gold.neighbourhood_pricing
WHERE room_type = 'Entire home/apt'
ORDER BY median_price DESC
LIMIT 10;


## 10. LIMITATIONS & RISKS

- Source data is a point-in-time scrape (Sep 2025); prices may have changed since
- neighbourhood_group column is always empty (Boston dataset lacks this grouping)
- avg_rating is null for 1 row (Leather District / Shared room) due to insufficient reviews
- Luxury listings can skew averages; prefer median_price for robust comparisons
- No seasonality captured; see gold.monthly_availability for temporal trends


## 11. GOVERNANCE & CONTACT

Owner: Kacper Kanak
Contact: PJATK MS Teams, Big Data course channel
Steward: Kacper Kanak (s24170)
Access policy: Private repo; professor added as collaborator
Marketplace location: MS Teams, Big Data course Data Marketplace channel
