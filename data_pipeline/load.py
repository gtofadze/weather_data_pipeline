from psycopg2.extras import execute_values

def load_data(df, cur, conn, tablename):

    rows = df.collect()
    data = [tuple(row) for row in rows]

    query = f"INSERT INTO {tablename} (city_id, date, temperature_c, temperature_f, wind_speed_km_h, wind_speed_m_s, weather_code) VALUES %s"
    execute_values(cur, query, data)

    conn.commit()