import pandas as pd


class Transform:

    def __init__(self, response_data, cities_info):

        self.response_data = response_data
        self.cities_info = cities_info
        self.dataframes = []  # list of dataframes for individual cities
        self.combined_dataframe = None  # combined dataframe of all cities
        self.transformed_dataframe = None

    def create_dataframes(self):

        response_data = self.response_data
        for id in response_data:

            date = response_data[id]["hourly"]["time"]
            temperature = response_data[id]["hourly"]["temperature_2m"]
            weather_code = response_data[id]["hourly"]["weather_code"]
            wind_speed = response_data[id]["hourly"]["wind_speed_10m"]

            df = pd.DataFrame(
                {
                    "date": date,
                    "temperature_c": temperature,
                    "weather_code": weather_code,
                    "wind_speed_km_h": wind_speed,
                }
            )

            df["id"] = id
            self.dataframes.append(df)
