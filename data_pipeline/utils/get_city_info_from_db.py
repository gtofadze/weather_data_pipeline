
def get_city_info_from_db(con, cur):
    data = cur.execute("SELECT * FROM cities_info")


    return data