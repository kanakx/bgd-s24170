from src.download import download_and_extract
from src.transform import run_sql_scripts
from src.load import load_csv_to_bronze


def main():
    print("=== Step 1: Download raw data ===")
    download_and_extract()

    print("\n=== Step 2: Create schemas ===")
    run_sql_scripts(["01_create_schemas.sql"])

    print("\n=== Step 3: Load CSVs into bronze layer ===")
    load_csv_to_bronze()

    print("\n=== Step 4: Transform bronze -> silver ===")
    run_sql_scripts(["03_silver_tables.sql"])

    print("\n=== Step 5: Build gold layer ===")
    run_sql_scripts(["04_gold_tables.sql"])

    print("\n=== Pipeline complete ===")


if __name__ == "__main__":
    main()
