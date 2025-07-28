from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, round, to_timestamp
from psycopg2.extras import execute_values
from database import establish_db_connection, close_db_connection, db_config

spark = SparkSession.builder.getOrCreate()


class DataTransformer:

    RECORD_TYPE = {1: "Historic", 2: "Forecast"}

    def __init__(self, response_data, cities, cities_info, record_type):

        self.record_type = record_type

        self.response_data = response_data

        self.cities = cities
        self.cities_info = cities_info

        self.dfs = []  # list of dataframes for individual cities

        self.combined_dataframe = 0  # combined dataframe of all cities
        self.transformed_dataframe = 0

    def create_dataframes(self):
        data = self.response_data
        cities = self.cities
        cities_info = self.cities_info

        for city in cities:

            id = cities_info[city]["id"]
            date = data[id]["hourly"]["time"]
            temperature = data[id]["hourly"]["temperature_2m"]
            weather_code = data[id]["hourly"]["weather_code"]
            wind_speed = data[id]["hourly"]["wind_speed_10m"]

            # prepare columns and define schema
            zipped_data_for_df = zip(date, temperature, weather_code, wind_speed)
            schema = ["date", "temperature_c", "weather_code", "wind_speed_km_h"]

            # create dataframes and add city id column
            df = spark.createDataFrame(zipped_data_for_df, schema)
            df = df.withColumn("city_id", lit(id))
            df = df.select(
                "city_id", "date", "temperature_c", "wind_speed_km_h", "weather_code"
            )

            self.dfs.append(df)

    def combine_dataframes(self):
        dfs = self.dfs

        df = dfs[0]
        for i in range(1, len(dfs)):

            df = df.union(dfs[i])

        self.combined_dataframe = df

    def transform_combined_dataframe(self):
        df = self.combined_dataframe
        df = df.withColumn("temperature_f", (col("temperature_c") * 9 / 5) + 32)
        df = df.withColumn("wind_speed_m_s", round(col("wind_speed_km_h") / 3.6, 2))

        df = df.select(
            "city_id",
            "date",
            "temperature_c",
            "temperature_f",
            "wind_speed_km_h",
            "wind_speed_m_s",
            "weather_code",
        )

        record_type = DataTransformer.RECORD_TYPE[self.record_type]
        df = df.withColumn("record_type", lit(record_type))

        self.transformed_dataframe = df

    def convert_datetime_strings(self):
        df = self.transformed_dataframe

        df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm"))

        self.transformed_dataframe = df

    def load(self, dbname, tablename):
        df = self.transformed_dataframe

        conn, cur = establish_db_connection(db_config, dbname=dbname)

        rows = df.collect()
        data = [tuple(row) for row in rows]

        query = f"INSERT INTO {tablename} (city_id, date, temperature_c, temperature_f, wind_speed_km_h, wind_speed_m_s, weather_code, record_type) VALUES %s"
        execute_values(cur, query, data)

        conn.commit()

        close_db_connection(conn, cur)
