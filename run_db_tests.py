from database.db_utils import prepare_database
from data_pipeline.parameters import cities_info, weather_code_map

prepare_database(cities_info, weather_code_map)
