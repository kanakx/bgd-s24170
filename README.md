# Airbnb NYC -- ELT Medallion Architecture

## Problem Statement

Analyzing short-term rental market dynamics in Boston to understand pricing factors, host performance, and neighbourhood desirability using Inside Airbnb open data.

## Dataset

Source: [Inside Airbnb -- Boston](https://insideairbnb.com/boston/) (September 2025 snapshot)

| File | Description | Approx Rows |
|------|-------------|-------------|
| listings.csv | Detailed listing info, host data, prices, review scores | ~4.4K |
| reviews.csv | Individual review entries with dates | ~233K |
| calendar.csv | Daily availability and pricing per listing | ~1.6M |

## Architecture

**Medallion Architecture** with three layers:

```
[CSV Files] --> Bronze (raw TEXT) --> Silver (typed, cleaned) --> Gold (aggregated/joined)
```

### Bronze Layer (Raw)
- `bronze.listings_raw` -- all columns as TEXT, exact CSV mirror
- `bronze.reviews_raw` -- all columns as TEXT
- `bronze.calendar_raw` -- all columns as TEXT

### Silver Layer (Cleaned)
- `silver.listings` -- proper types (NUMERIC, DATE, BOOLEAN), price cleaned ($, commas removed), nulls handled, deduped
- `silver.reviews` -- proper DATE types, null rows dropped
- `silver.calendar` -- price cleaned, boolean availability, proper dates

### Gold Layer (Business)
- `gold.neighbourhood_pricing` -- AVG/median price, listing count, avg rating per neighbourhood + room type (aggregation)
- `gold.host_performance` -- host metrics joined with verified review counts (JOIN listings + reviews)
- `gold.monthly_availability` -- monthly availability rate by neighbourhood (JOIN calendar + listings)

## Setup & Run

### Prerequisites
- Docker
- Python 3.10+

### Quick Start
```bash
# 1. Start PostgreSQL
docker compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run the full ELT pipeline
python main.py
```

### Tear Down
```bash
docker compose down -v
```

## Data Quality Risks

### 1. Missing Values / Nulls
Many listings lack `review_scores_rating`, `host_response_rate`, and `neighbourhood_group`. Approximately 25% of listings have no review scores at all. This affects silver layer completeness and can skew gold layer aggregations (e.g., average neighbourhood rating may only reflect a subset of listings).

### 2. Price Format Inconsistency & Outliers
Price is stored as a string (`"$1,250.00"`) requiring parsing to remove `$` and `,`. Some listings have `$0.00` (likely errors or placeholders) or extreme values (>$10,000/night) that distort mean-based aggregations. Calendar prices may differ from listing base prices, creating potential confusion in price analysis.

### 3. Temporal Data Staleness & Duplicates
Calendar data represents a point-in-time snapshot (scraped on a single date) and may not reflect real-time availability. Reviews may contain duplicate entries from overlapping scrape runs. Listings marked as "active" could be abandoned profiles that haven't been delisted, inflating listing counts and distorting availability metrics.

## Project Structure

```
bgd-s24170/
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── main.py                    # Full pipeline orchestrator
├── data/raw/                  # Downloaded CSVs (gitignored)
├── sql/
│   ├── 01_create_schemas.sql
│   ├── 02_bronze_tables.sql
│   ├── 03_silver_tables.sql
│   └── 04_gold_tables.sql
└── src/
    ├── config.py              # DB connection (SQLAlchemy)
    ├── download.py            # Fetch CSVs from Inside Airbnb
    ├── load.py                # Load CSVs into bronze
    └── transform.py           # Execute SQL transformations
```
