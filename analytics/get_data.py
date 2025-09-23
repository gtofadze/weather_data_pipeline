from database import establish_db_connection, close_db_connection, db_config


def get_data_from_db(db_name):

    data = {}

    conn, cur = establish_db_connection(db_config, dbname=db_name)

    cur.execute("SELECT * FROM weather_codes")
    weather_data = cur.fetchall()
    weather_data_headers = [desc[0] for desc in cur.description]

    cur.execute("SELECT * FROM cities_info")
    cities_info = cur.fetchall()
    cities_info_headers = [desc[0] for desc in cur.description]

    cur.execute("SELECT * FROM weather_codes")
    weather_codes = cur.fetchall()
    weather_codes_headers = [desc[0] for desc in cur.description]

    close_db_connection(conn, cur)

    data["weather_data"] = {"headers": weather_data_headers, "data": weather_data}
    data["cities_info"] = {"headers": cities_info_headers, "data": cities_info}
    data["weather_codes"] = {"headers": weather_codes_headers, "data": weather_codes}

    return data
