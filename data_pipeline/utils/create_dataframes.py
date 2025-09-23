import pandas as pd


def create_weather_data_dataframes(data):

    dataframes = []

    for id in data:

        date = data[id]["hourly"]["time"]
        temperature = data[id]["hourly"]["temperature_2m"]
        weather_code = data[id]["hourly"]["weather_code"]
        wind_speed = data[id]["hourly"]["wind_speed_10m"]

        df = pd.DataFrame(
            {
                'date': date,
                'temperature_c': temperature,
                'weather_code': weather_code,
                'wind_speed_km_h': wind_speed
            }
        )