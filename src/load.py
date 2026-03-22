import os
import pandas as pd
from src.config import engine

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

TABLES = {
    "listings.csv": ("bronze", "listings_raw"),
    "reviews.csv": ("bronze", "reviews_raw"),
    "calendar.csv": ("bronze", "calendar_raw"),
}


def load_csv_to_bronze():
    for csv_file, (schema, table) in TABLES.items():
        csv_path = os.path.join(RAW_DIR, csv_file)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found. Run download first.")

        print(f"  Loading {csv_file} -> {schema}.{table} ...")
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
        print(f"  [done] {len(df):,} rows loaded into {schema}.{table}")


if __name__ == "__main__":
    load_csv_to_bronze()
