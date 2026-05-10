import os
import csv
import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

TOPIC_MAP = {
    "listings.csv": "airbnb.listings",
    "reviews.csv": "airbnb.reviews",
    "calendar.csv": "airbnb.calendar",
}


def wait_for_kafka(retries=30, delay=2):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            p.close()
            return
        except Exception:
            print(f"  Waiting for Kafka ({i+1}/{retries})...")
            time.sleep(delay)
    raise ConnectionError("Kafka not available")


def produce(csv_file=None):
    files = {csv_file: TOPIC_MAP[csv_file]} if csv_file else TOPIC_MAP

    wait_for_kafka()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=32768,
    )

    for filename, topic in files.items():
        csv_path = os.path.join(RAW_DIR, filename)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found")

        print(f"  Producing {filename} -> {topic} ...")
        count = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                producer.send(topic, value=dict(row))
                count += 1
                if count % 50000 == 0:
                    print(f"    {count:,} rows sent")

        producer.flush()
        print(f"  [done] {count:,} rows -> {topic}")

    producer.close()


if __name__ == "__main__":
    produce()
