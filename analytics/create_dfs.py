from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def create_dfs(data):

    dataframes = {}

    weather_df = spark.createDataFrame(
        data["weather_data"]["data"], data["weather_data"]["headers"]
    )
    dataframes["weather_df"] = weather_df

    cities_info_df = spark.createDataFrame(
        data["cities_info"]["data"], data["cities_info"]["headers"]
    )
    dataframes["cities_info"] = cities_info_df

    weather_codes = spark.createDataFrame(
        data["weather_codes"]["data"], data["weather_codes"]["headers"]
    )
    dataframes["cities_info"] = weather_codes

    return dataframes
