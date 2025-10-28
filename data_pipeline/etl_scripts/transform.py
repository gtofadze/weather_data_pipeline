import pandas as pd
import numpy as np


class Transform:

    def __init__(self, response_data):

        self.response_data = response_data
        self.transformed_dataframe = None

    def create_dataframes(self):

        response_data = self.response_data
        for id in response_data:

            dataframes = []

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

            df["city_id"] = id
            dataframes.append(df)

            return dataframes

    def transform(self):

        dataframes = self.create_dataframes()
        df = pd.concat(dataframes, ignore_index=True)
        df["wind_speed_m_s"] = df["wind_speed_km_h"] / 3.6
        df["temperature_f"] = (df["temperature_c"] * 9 / 5) + 32
        df["date"] = pd.to_datetime(df["date"])

        conditions = [
            df["wind_speed_m_s"] <= 0.2,
            df["wind_speed_m_s"] <= 1.5,
            df["wind_speed_m_s"] <= 3.3,
            df["wind_speed_m_s"] <= 5.4,
            df["wind_speed_m_s"] <= 7.9,
            df["wind_speed_m_s"] <= 10.7,
            df["wind_speed_m_s"] <= 13.8,
            df["wind_speed_m_s"] <= 17.1,
            df["wind_speed_m_s"] <= 20.7,
            df["wind_speed_m_s"] <= 24.4,
            df["wind_speed_m_s"] <= 28.4,
            df["wind_speed_m_s"] <= 32.6,
            df["wind_speed_m_s"] > 32.6
        ]

        choices = list(range(13))

        df["beaufort_scale"] = np.select(conditions, choices, default=None)

        columns_after_reordering = [
            "city_id",
            "date",
            "temperature_c",
            "temperature_f",
            "wind_speed_km_h",
            "wind_speed_m_s",
            "beaufort_scale",
            "weather_code",
        ]

        df = df[columns_after_reordering]

        self.transformed_dataframe = df
