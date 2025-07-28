from data_pipeline import extract_data, DataTransformer
from data_pipeline import (
    params,
    cities_info,
    cities,
    historic_data_base_url,
    forecast_data_base_url,
    weather_code_map,
)
from database import (
    prepare_database,
    db_config,
)

from run_db_tests import delete_db

start_date = "2025-07-01"
end_date = "2025-07-02"


def execute_data_pipeline(record_type, start_date, end_date):

    # for historic record type enter 1 for forecast enter 2 in "record_type" variable

    if record_type == 1:
        base_url = historic_data_base_url
    elif record_type == 2:
        base_url = forecast_data_base_url

    delete_db(db_config, "postgres")
    prepare_database(cities_info, weather_code_map)

    extracted_data = extract_data(
        cities_info, cities, base_url, params, start_date, end_date
    )

    data_transformer = DataTransformer(extracted_data, cities, cities_info, record_type)

    data_transformer.create_dataframes()
    data_transformer.combine_dataframes()
    data_transformer.transform_combined_dataframe()
    data_transformer.convert_datetime_strings()

    # data_transformer.dfs[0].show(3)
    # data_transformer.dfs[2].show(3)
    data_transformer.transformed_dataframe.show(10, truncate=False)

    data_transformer.load("weather", "weather_data")


execute_data_pipeline(1, start_date, end_date)  # for historic data
execute_data_pipeline(2, start_date, end_date)  # for forecast data
