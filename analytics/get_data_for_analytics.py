import pandas as pd


def get_weather_df_from_db(engine):
    df = pd.read_sql("weather_data", engine)
    return df


def get_cities_df_from_db(engine):
    df = pd.read_sql("cities_info", engine)
    return df


def get_weather_codes_df_from_db(engine):
    df = pd.read_sql("weather_codes", engine)
    return df


def get_beaufort_scale_df_from_db(engine):
    df = pd.read_sql("beaufort_scale", engine)
    return df
