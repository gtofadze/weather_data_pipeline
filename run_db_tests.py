from database import prepare_database, db_config
from data_pipeline import cities_info, weather_code_map
import psycopg2


def delete_db(db_config, dbname):
    conn = psycopg2.connect(**db_config, dbname=dbname)
    conn.autocommit = True  # Required to execute DROP DATABASE outside a transaction

    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS my_database_name")

    cur.close()
    conn.close()


delete_db(db_config, "postgres")
prepare_database(cities_info, weather_code_map)
