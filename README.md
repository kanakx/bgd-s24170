# Airbnb Boston -- ELT Medallion Architecture

## Problem Statement

Analyzing short-term rental market dynamics in Boston to understand pricing factors, host performance, and neighbourhood desirability using Inside Airbnb open data.

## Dataset

Source: [Inside Airbnb -- Boston](https://insideairbnb.com/boston/) (September 2025 snapshot)

| File | Description | Approx Rows |
|------|-------------|-------------|
| listings.csv | Detailed listing info, host data, prices, review scores | ~4.4K |
| reviews.csv | Individual review entries with dates | ~233K |
| calendar.csv | Daily availability and pricing per listing | ~1.6M |

## Pipeline Architecture

```mermaid
flowchart LR
    subgraph src["Inside Airbnb"]
        CSV["listings.csv\nreviews.csv\ncalendar.csv"]
    end

    subgraph docker["Docker Compose"]
        subgraph airflow["Apache Airflow (@daily)"]
            DAG["DAG: airbnb_elt_pipeline"]
            T1["download_csvs"]
            T2["produce_to_kafka"]
            T3["consume_to_bronze"]
            T4["dbt_run_staging"]
            T5["dbt_run_marts"]
            T1 --> T2 --> T3 --> T4 --> T5
        end

        subgraph kafka["Apache Kafka (KRaft)"]
            KT1["airbnb.listings"]
            KT2["airbnb.reviews"]
            KT3["airbnb.calendar"]
        end

        subgraph pg["PostgreSQL"]
            subgraph bronze["Bronze Layer"]
                B1["listings_raw"]
                B2["reviews_raw"]
                B3["calendar_raw"]
            end
            subgraph silver["Silver Layer"]
                S1["stg_listings"]
                S2["stg_reviews"]
                S3["stg_calendar"]
            end
            subgraph gold["Gold Layer"]
                G1["neighbourhood_pricing"]
                G2["host_performance"]
                G3["monthly_availability"]
            end
        end
    end

    CSV -->|"Producer\n(kafka-python)"| kafka
    kafka -->|"Consumer\n(kafka-python)"| bronze
    bronze -->|"dbt\nstaging"| silver
    silver -->|"dbt\nmarts"| gold
```

### Tool Stack

| Role | Tool |
|------|------|
| Orchestration | Apache Airflow (DAG-based scheduling + UI, `@daily`) |
| Queue System | Apache Kafka (KRaft mode, single-node broker) |
| Processing Engine | PySpark (local mode, used for download) |
| Transformations | dbt-postgres (staging + marts models) |
| Data Warehouse | PostgreSQL (Dockerized) |

### Data Flow

1. **Download** -- CSVs fetched from Inside Airbnb (skips if already present)
2. **Produce** -- Python producer reads CSVs row-by-row, publishes JSON to Kafka topics (`airbnb.listings`, `airbnb.reviews`, `airbnb.calendar`)
3. **Consume** -- Python consumer reads from Kafka topics, bulk-inserts into PostgreSQL bronze tables via `COPY`
4. **dbt staging** -- Bronze -> Silver: type casting, price cleaning, null handling
5. **dbt marts** -- Silver -> Gold: aggregations and JOINs

### Medallion Layers

**Bronze (Raw)** -- Consumer loads from Kafka as-is, all text columns

**Silver (Cleaned)** -- dbt staging models: proper types, price parsing, null handling, deduplication

**Gold (Business)** -- dbt mart models: aggregations and JOINs for analytics

| Gold Table | Description | Source |
|------------|-------------|--------|
| `neighbourhood_pricing` | AVG/median price, count, avg rating per neighbourhood + room type | stg_listings |
| `host_performance` | Host metrics with verified review counts | stg_listings JOIN stg_reviews |
| `monthly_availability` | Monthly availability rate by neighbourhood | stg_calendar JOIN stg_listings |

## Setup & Run

### Prerequisites
- Docker + Docker Compose

### Quick Start
```bash
# 1. Copy environment file
cp .env.example .env

# 2. Start all services (PostgreSQL + Kafka + Airflow)
docker compose up -d

# 3. Open Airflow UI -- DAG runs automatically on @daily schedule
open http://localhost:8080
# Login: admin / admin (auto-created by Airflow standalone)
# To trigger manually: navigate to airbnb_elt_pipeline -> Trigger DAG
```

### CLI Alternative (without UI)
```bash
docker compose exec airflow airflow dags trigger airbnb_elt_pipeline
```

### Tear Down
```bash
docker compose down -v
```

### Local Development (without Docker Airflow)
```bash
docker compose up postgres kafka -d
pip install -r requirements.txt

python spark/ingest.py download
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 python streaming/producer.py
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 python streaming/consumer.py
cd dbt_project && dbt run --profiles-dir .
```

## Idempotency

| Layer | Strategy |
|-------|----------|
| Download | Skips if CSV already exists on disk |
| Producer | Re-sending rows to Kafka is safe (consumer always reads from beginning) |
| Consumer | `DROP + CREATE` bronze tables before loading (clean slate per run) |
| Silver/Gold | dbt manages table lifecycle (no manual DROP CASCADE) |
| Schemas | `CREATE SCHEMA IF NOT EXISTS` (never drops) |
| DAG | `@daily` schedule, re-triggerable, produces same result |

## Data Quality Risks

### 1. Missing Values / Nulls
Many listings lack `review_scores_rating`, `host_response_rate`, and `neighbourhood_group`. Approximately 25% of listings have no review scores at all. This affects silver layer completeness and can skew gold layer aggregations.

### 2. Price Format Inconsistency & Outliers
Price is stored as a string (`"$1,250.00"`) requiring parsing. Some listings have `$0.00` or extreme values (>$10,000/night) that distort mean-based aggregations.

### 3. Temporal Data Staleness & Duplicates
Calendar data is a point-in-time snapshot. Reviews may contain duplicates from overlapping scrape runs. Listings marked "active" could be abandoned profiles.

## Project Structure

```
bgd-s24170/
├── docker-compose.yml              # PostgreSQL + Kafka + Airflow
├── airflow/Dockerfile              # Airflow image with Java + PySpark + dbt + kafka-python
├── .env.example                    # Environment template
├── requirements.txt                # Python dependencies
│
├── dags/
│   └── airbnb_elt_pipeline.py      # Airflow DAG (5 tasks, @daily schedule)
│
├── streaming/
│   ├── producer.py                 # CSV -> Kafka topics (JSON messages)
│   └── consumer.py                 # Kafka topics -> PostgreSQL bronze (COPY)
│
├── spark/
│   └── ingest.py                   # PySpark: download CSVs + legacy bronze loader
│
├── dbt_project/
│   ├── dbt_project.yml             # dbt configuration
│   ├── profiles.yml                # PostgreSQL connection
│   ├── macros/                     # Schema name override
│   └── models/
│       ├── staging/                # Bronze -> Silver (stg_listings, stg_reviews, stg_calendar)
│       └── marts/                  # Silver -> Gold (neighbourhood_pricing, host_performance, monthly_availability)
│
├── diagrams/                       # Architecture diagrams (ERD + high-level)
├── sql/                            # Original SQL scripts (Assignment 1 reference)
├── src/                            # Original Python modules (Assignment 1 reference)
└── main.py                         # Original entry point (Assignment 1 reference)
```
