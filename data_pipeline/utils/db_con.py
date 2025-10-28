import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os


def establish_db_connection(
    dbname="postgres", autocommit=False, isolation_level_autocommit=False
):

    load_dotenv()
    HOST = os.getenv("HOST")
    USER = os.getenv("USER")
    PORT = os.getenv("PORT")
    PASSWORD = os.getenv("PASSWORD")

    db_config = {
        "user": USER,
        "password": PASSWORD,
        "host": HOST,
        "port": PORT
    }

    conn = psycopg2.connect(**db_config, dbname=dbname)

    if isolation_level_autocommit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    if autocommit:
        conn.autocommit = True
    return conn, cur


def close_db_connection(conn, cur):
    cur.close()
    conn.close()
