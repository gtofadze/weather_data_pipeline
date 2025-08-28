from database import establish_db_connection, close_db_connection, db_config

def get_data_from_db(db_name):

    data = {}
    
    conn, cur = establish_db_connection(db_config, dbname=db_name)

    weather_data = cur.execute("SELECT * FROM weather_codes").fetchall()
    cities_info = cur.execute("SELECT * FROM cities_info").fetchall()
    weather_codes = cur.execute("SELECT * FROM weather_codes").fetchall()

    close_db_connection(conn, cur)

    data['weather_data'] = weather_data
    data['cities_info'] = cities_info
    data['weather_codes'] = weather_codes

    return data

get_data_from_db('weather')
