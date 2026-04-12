import os
import gzip
import shutil
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

BASE_URL = "https://data.insideairbnb.com/united-states/ma/boston/2025-09-23/data"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

FILES = {
    "listings.csv.gz": "listings.csv",
    "calendar.csv.gz": "calendar.csv",
    "reviews.csv.gz": "reviews.csv",
}

BRONZE_TABLES = {
    "listings.csv": "listings_raw",
    "reviews.csv": "reviews_raw",
    "calendar.csv": "calendar_raw",
}

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "airbnb")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airbnb_pass")
DB_NAME = os.getenv("POSTGRES_DB", "airbnb_nyc")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def download_csvs():
    os.makedirs(RAW_DIR, exist_ok=True)
    for gz_name, csv_name in FILES.items():
        csv_path = os.path.join(RAW_DIR, csv_name)
        if os.path.exists(csv_path):
            print(f"  [skip] {csv_name} already exists")
            continue
        url = f"{BASE_URL}/{gz_name}"
        gz_path = os.path.join(RAW_DIR, gz_name)
        print(f"  Downloading {gz_name} ...")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  Extracting to {csv_name} ...")
        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(gz_path)
        print(f"  [done] {csv_name}")


def load_bronze(spark):
    jdbc_props = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
    }
    for csv_name, table_name in BRONZE_TABLES.items():
        csv_path = os.path.join(RAW_DIR, csv_name)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found. Run download first.")

        print(f"  Loading {csv_name} -> bronze.{table_name} ...")
        df = spark.read.csv(csv_path, header=True, inferSchema=False, multiLine=True, escape='"')
        df.write.jdbc(
            url=JDBC_URL,
            table=f"bronze.{table_name}",
            mode="overwrite",
            properties=jdbc_props,
        )
        print(f"  [done] {df.count():,} rows -> bronze.{table_name}")


def ensure_schemas(spark):
    jdbc_props = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
    }
    import psycopg2
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()
    for schema in ("bronze", "silver", "gold"):
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.close()
    conn.close()
    print("  [done] schemas ensured")


def create_spark():
    spark = (
        SparkSession.builder.appName("airbnb_bronze_load")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    import sys
    step = sys.argv[1] if len(sys.argv) > 1 else "all"

    if step == "download":
        print("=== Download CSVs ===")
        download_csvs()
    elif step == "bronze":
        spark = create_spark()
        try:
            print("=== Ensure schemas ===")
            ensure_schemas(spark)
            print("\n=== Load bronze layer ===")
            load_bronze(spark)
        finally:
            spark.stop()
    else:
        print("=== Download CSVs ===")
        download_csvs()
        spark = create_spark()
        try:
            print("\n=== Ensure schemas ===")
            ensure_schemas(spark)
            print("\n=== Load bronze layer ===")
            load_bronze(spark)
        finally:
            spark.stop()

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
