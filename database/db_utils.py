import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# read database config details from file
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, "db_details.txt")

print(file_path, "=" * 50)

try:
    with open(file_path, "r") as file:
        lines = file.readlines()
except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

cleared_lines = [line.strip() for line in lines]

print(cleared_lines)

db_config = {
    "host": cleared_lines[0],
    "user": cleared_lines[1],
    "port": int(cleared_lines[2]),
    "password": cleared_lines[3],
}


def establish_db_connection(db_config, dbname="postgres"):
    conn = psycopg2.connect(**db_config, dbname=dbname)
    cur = conn.cursor()
    return conn, cur


def close_db_connection(conn, cur):
    cur.close()
    conn.close()


# check if database exists
def check_if_db_exists():
    conn = psycopg2.connect(**db_config, dbname="postgres")

    conn.autocommit = True
    cur = conn.cursor()

    db_name = "weather"
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone() is not None

    cur.close()
    conn.close()

    return exists


# Creating database for weather data
def create_database():

    conn = psycopg2.connect(**db_config, dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()

    cur.execute("CREATE DATABASE weather")

    cur.close()
    conn.close()


# Creating tables for weather data
def create_tables():

    conn, cur = establish_db_connection(db_config, dbname="weather")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS cities_info(
        id INT PRIMARY KEY,
        name VARCHAR(255),
        lon FLOAT,
        lat FLOAT
        )"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS weather_codes(
                code INT PRIMARY KEY,
                description VARCHAR(255)
                )"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS weather_data(
                city_id INT REFERENCES cities_info(id),
                date TIMESTAMP,
                temperature_c FLOAT,
                temperature_f FLOAT,
                wind_speed_km_h FLOAT,
                wind_speed_m_s FLOAT,
                weather_code INT REFERENCES weather_codes(code),
                record_type VARCHAR(255)
                )"""
    )

    conn.commit()

    close_db_connection(conn, cur)


def populate_dimensions_data(cities_info, weather_codes):

    conn, cur = establish_db_connection(db_config, dbname="weather")

    for city in cities_info:
        id = cities_info[city]["id"]
        name = city
        lon = cities_info[city]["lon"]
        lat = cities_info[city]["lat"]

        values = (id, name, lon, lat)

        query_string = """INSERT INTO cities_info (id, name, lon, lat) 
        VALUES (%s, %s, %s, %s)"""
        cur.execute(query_string, values)

    for code in weather_codes:
        description = weather_codes[code]

        values = (code, description)

        query_string = """INSERT INTO weather_codes (code, description) 
        VALUES (%s, %s)"""
        cur.execute(query_string, values)

    conn.commit()

    close_db_connection(conn, cur)


def prepare_database(cities_info, weather_codes):
    if not check_if_db_exists():
        create_database()
        create_tables()
        populate_dimensions_data(cities_info, weather_codes)
    return
