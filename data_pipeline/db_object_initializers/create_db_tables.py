def create_tables(conn, cur):

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
        """CREATE TABLE IF NOT EXISTS beaufort_scale(
                beaufort_number INT PRIMARY_KEY,
                lower_bound_km_h INT,
                upper_bound_km_h INT,
                description VARCHAR(255),
                land_conditions VARCHAR(255)
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
