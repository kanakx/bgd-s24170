import os
import gzip
import shutil
import requests

BASE_URL = "https://data.insideairbnb.com/united-states/ma/boston/2025-09-23/data"
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

FILES = {
    "listings.csv.gz": "listings.csv",
    "calendar.csv.gz": "calendar.csv",
    "reviews.csv.gz": "reviews.csv",
}


def download_and_extract():
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


if __name__ == "__main__":
    download_and_extract()
