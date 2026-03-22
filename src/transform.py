import os
from sqlalchemy import text
from src.config import engine

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

SCRIPTS = [
    "01_create_schemas.sql",
    "02_bronze_tables.sql",
    "03_silver_tables.sql",
    "04_gold_tables.sql",
]


def run_sql_scripts(scripts=None):
    scripts = scripts or SCRIPTS
    for script_name in scripts:
        path = os.path.join(SQL_DIR, script_name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQL script not found: {path}")

        print(f"  Running {script_name} ...")
        with open(path, "r") as f:
            sql = f.read()

        with engine.begin() as conn:
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))

        print(f"  [done] {script_name}")


if __name__ == "__main__":
    run_sql_scripts()
