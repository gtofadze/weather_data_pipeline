from extract import extract_data
from transform import DataTransformer
from spark_session import get_spark
from parameters import (
    params,
    cities_info,
    cities,
    historic_data_base_url,
    forecast_data_base_url,
)

spark = get_spark()

start_date = "2025-07-01"
end_date = "2025-07-02"

def execute_data_pipeline(spark, record_type, start_date, end_date):

    # for historic record type enter 1 for forecast enter 2 in "record_type" variable

    if record_type == 1:
        base_url = historic_data_base_url
    elif record_type == 2:
        base_url = forecast_data_base_url

    extracted_data = extract_data(
        cities_info, cities, base_url, params, start_date, end_date
    )

    data_transformer = DataTransformer(spark, extracted_data, cities, cities_info, record_type)

    data_transformer.create_dataframes()
    data_transformer.combine_dataframes()
    data_transformer.transform_combined_dataframe()

    # data_transformer.dfs[0].show(3)
    # data_transformer.dfs[2].show(3)
    data_transformer.transformed_dataframe.show(50, truncate=False)


execute_data_pipeline(spark, 1, start_date, end_date) # for historic data
execute_data_pipeline(spark, 2, start_date, end_date) # for forecast data
