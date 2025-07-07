from extract import extract_data
from transform import DataTransformer
from parameters import (
    params,
    cities_info,
    cities,
    historic_data_base_url,
    forecast_data_base_url,
)

def execute_data_pipeline():

    extracted_data = extract_data(
        cities_info, cities, historic_data_base_url, params, "2025-07-01", "2025-07-02"
    )

    data_transformer = DataTransformer(extracted_data, cities, cities_info, 1)

    data_transformer.create_historic_dataframes()
    data_transformer.combine_dataframes()
    data_transformer.transform_combined_dataframe()

    data_transformer.dfs[0].show(3)
    data_transformer.dfs[2].show(3)
    data_transformer.transformed_dataframe.show(50, truncate=False)

execute_data_pipeline()