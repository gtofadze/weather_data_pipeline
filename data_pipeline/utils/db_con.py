import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os


def establish_db_connection(
    dbname="postgres", autocommit=False, isolation_level_autocommit=False
):

    load_dotenv()

    db_config = {
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "host": os.getenv("HOST"),
        "port": os.getenv("PORT")
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
