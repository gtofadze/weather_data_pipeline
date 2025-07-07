import requests
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, round

spark = SparkSession.builder.getOrCreate()

cities_info = {
    "Tbilisi": {"id": 1, "lon": 44.8271, "lat": 41.7151},
    "Batumi": {"id": 2, "lon": 41.6367, "lat": 41.6168},
    "Zugdidi": {"id": 3, "lon": 41.8709, "lat": 42.5090},
    "Kutaisi": {"id": 4, "lon": 42.7180, "lat": 42.2480},
    "Gori": {"id": 5, "lon": 44.1083, "lat": 41.9862},
}

cities = ["Tbilisi", "Batumi", "Zugdidi", "Kutaisi", "Gori"]

historic_data_base_url = "https://archive-api.open-meteo.com/v1/archive"
forecast_data_base_url = "https://api.open-meteo.com/v1/forecast"

params = {
    "start_date": "2025-07-01",
    "end_date": "2025-07-02",
    "longitude": 44.8271,
    "latitude": 41.7151,
    "hourly": ["temperature_2m", "weather_code", "wind_speed_10m"],
    "timezone": "GMT",
    "temporal_resolution": "hourly_6",
}

weather_code_map = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def extract_data(cities_info, cities, base_url, params, start_date, end_date):

    extracted_data = {}

    for city in cities:
        id = cities_info[city]["id"]
        lon = cities_info[city]["lon"]
        lat = cities_info[city]["lat"]

        params["lon"] = lon
        params["lat"] = lat
        params["start_date"] = start_date
        params["end_date"] = end_date

        r = requests.get(base_url, params=params)
        r = r.json()

        extracted_data[id] = r

    return extracted_data


extracted_data = extract_data(
    cities_info, cities, historic_data_base_url, params, "2025-07-01", "2025-07-02"
)

pprint(extracted_data)

# transform ---------------------------------------------------------


class DataTransformer:

    RECORD_TYPE = {1: "Historic", 2: "Forecast"}

    def __init__(self, response_historic_data, cities, cities_info, record_type):
        self.record_type = record_type

        self.response_histiric_data = response_historic_data

        self.cities = cities
        self.cities_info = cities_info

        self.dfs = []  # list of dataframes for individual cities

        self.combined_dataframe = 0  # combined dataframe of all cities
        self.transformed_dataframe = 0

    def create_historic_dataframes(self):
        data = self.response_histiric_data
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

            # create dataframes and add city id culumn
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


data_transformer = DataTransformer(extracted_data, cities, cities_info, 1)

data_transformer.create_historic_dataframes()
data_transformer.combine_dataframes()
data_transformer.transform_combined_dataframe()

data_transformer.dfs[0].show(3)
data_transformer.dfs[2].show(3)
# data_transformer.combined_dataframe.show()
data_transformer.transformed_dataframe.show(50, truncate=False)
