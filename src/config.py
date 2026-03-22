import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL

load_dotenv()


def get_engine():
    url = URL.create(
        drivername="postgresql",
        username=os.getenv("POSTGRES_USER", "airbnb"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "airbnb_nyc"),
    )
    return create_engine(url)


engine = get_engine()
