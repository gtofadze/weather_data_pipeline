import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def establish_db_connection(
    dbname="postgres", autocommit=False, isolation_level_autocommit=False, **db_config
):
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
