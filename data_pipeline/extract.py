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
          'hourly': ['temperature_2m' ,'weather_code'],
          'timezone': 'GMT',
          'temporal_resolution': 'hourly_6'}

#  ,'temporal_resolution': 'hourly_6'

response = requests.get(base_url, params=params)
response = response.json()
#pprint(type(response))
pprint(response)

data_for_df = list(zip(response['hourly']['time'],
                       response['hourly']['temperature_2m'],
                       response['hourly']['weather_code']))

schema = ["time", "temperature_c", "weather_code"]

df = spark.createDataFrame(data_for_df, schema)
df.show(3)
