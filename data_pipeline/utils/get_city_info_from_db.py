
def get_city_info_from_db(cur):
    cur.execute("SELECT * FROM cities_info")

    return cur.fetchall()