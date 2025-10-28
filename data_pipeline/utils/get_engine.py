from sqlalchemy import create_engine
from sqlalchemy import URL
from dotenv import load_dotenv
import os


def get_engine(db):

    load_dotenv()
    HOST = os.getenv("HOST")
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")

    url_object = URL.create(
        "postgresql+psycopg2",
        username=USER,
        password=PASSWORD,
        host=HOST,
        database=db,
    )

    engine = create_engine(url_object)

    return engine
