import requests
from pprint import pprint

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

cities_info = {
    "Tbilisi": {"id":1, "lon": 44.8271, "lat": 41.7151},
    "Batumi": {"id":2, "lon": 41.6367, "lat": 41.6168},
    "Zugdidi": {"id":3, "lon": 41.8709, "lat": 42.5090},
    "Kutaisi": {"id":4, "lon": 42.7180, "lat": 42.2480},
    "Gori": {"id":5, "lon": 44.1083, "lat": 41.9862}
}

cities = ['Tbilisi', 'Batumi', 'Zugdidi', 'Kutaisi', 'Gori']

base_url ="https://archive-api.open-meteo.com/v1/archive"
params = {'start_date': '2025-07-01', 
          'end_date':'2025-07-02',
          'longitude': 44.8271,
          'latitude': 41.7151,
          'hourly': ['temperature_2m' ,'weather_code', 'wind_speed_10m'],
          'timezone': 'GMT',
          'temporal_resolution': 'hourly_6'}

#response = requests.get(base_url, params=params)
#response = response.json()
#pprint(type(response))
#pprint(response)

# data_for_df = list(zip(response['hourly']['time'],
#                        response['hourly']['temperature_2m'],
#                        response['hourly']['weather_code']))

# schema = ["time", "temperature_c", "weather_code"]

# df = spark.createDataFrame(data_for_df, schema)
# df.show(3)

def extract_data(cities_info, cities, base_url, params, start_date, end_date):
    
    extracted_data = {}

    for city in cities:
        id = cities_info[city]['id']
        lon = cities_info[city]['lon']
        lat = cities_info[city]['lat']

        params['lon'] = lon
        params['lat'] = lat
        params['start_date'] = start_date
        params['end_date'] = end_date

        r = requests.get(base_url, params=params)
        r = r.json()

        extracted_data[id] = r
        
    return extracted_data
    
extracted_data = extract_data(cities_info, cities, base_url, params,'2025-07-01', '2025-07-02')

pprint(extracted_data)

# transform ---------------------------------------------------------

class DataTransformer:
    def __init__(self, response_historic_data, cities, cities_info):
        self.response_histiric_data = response_historic_data
        self.cities = cities
        self.cities_info = cities_info
        self.dfs = []
        self.transformed_df = 0

    def create_historic_dataframes(self):
        data = self.response_histiric_data
        cities = self.cities
        cities_info = self.cities_info 

        for city in cities:
            id = cities_info[city]

            date = data[city]['hourly']['time']
            temperature = data[city]['hourly']['temperature_2m']
            weather_code = data[city]['hourly']['weather_code']

            zipped_data_for_df = zip(date, temperature, weather_code)
            schema = ["time", "temperature_c", "weather_code"]

            df = spark.createDataFrame(zipped_data_for_df, schema)
            df = df.withColumn('city_id', id)
            
            self.dfs.append(df)

data_transformer = DataTransformer(extracted_data, cities, cities_info)
data_transformer.create_historic_dataframes()

data_transformer.dfs.show(3)

            