def get_city_info_from_db(cur):
    cur.execute("SELECT * FROM cities_info")
    data_pre = cur.fetchall()

    data_post = []

    for city in data_pre:
        d = {"id": city[0], "lon": city[2], "lat": city[3]}
        data_post.append(d)

    return data_post
