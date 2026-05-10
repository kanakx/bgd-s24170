import os
import json
import time
import psycopg2
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "airbnb")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airbnb_pass")
DB_NAME = os.getenv("POSTGRES_DB", "airbnb_nyc")

TOPIC_TABLE = {
    "airbnb.listings": "bronze.listings_raw",
    "airbnb.reviews": "bronze.reviews_raw",
    "airbnb.calendar": "bronze.calendar_raw",
}


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def ensure_schemas():
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    for schema in ("bronze", "silver", "gold"):
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.close()
    conn.close()


def consume_topic(topic, table):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        group_id=f"bronze-loader-{topic}",
        enable_auto_commit=False,
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        print(f"  [skip] Topic {topic} has no partitions")
        consumer.close()
        return

    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)

    print(f"  Consuming {topic} -> {table} ...")
    rows = []
    columns = None
    for msg in consumer:
        row = msg.value
        if columns is None:
            columns = list(row.keys())
        rows.append(row)
        if len(rows) % 50000 == 0:
            print(f"    {len(rows):,} rows read")

    consumer.close()

    if not rows:
        print(f"  [skip] No messages in {topic}")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    schema, tbl = table.split(".")
    col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.execute(f"CREATE TABLE {table} ({col_defs})")

    import io
    buf = io.StringIO()
    for row in rows:
        line = "\t".join((row.get(c, "") or "").replace("\t", " ").replace("\n", " ") for c in columns)
        buf.write(line + "\n")
    buf.seek(0)

    col_list = ", ".join(f'"{c}"' for c in columns)
    copy_sql = f'COPY {table} ({col_list}) FROM STDIN WITH (FORMAT text, DELIMITER E\'\\t\', NULL \'\')'
    cur.copy_expert(copy_sql, buf)

    conn.commit()
    cur.close()
    conn.close()
    print(f"  [done] {len(rows):,} rows -> {table}")


def consume_all():
    ensure_schemas()
    for topic, table in TOPIC_TABLE.items():
        consume_topic(topic, table)


if __name__ == "__main__":
    consume_all()
