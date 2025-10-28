from etl_scripts import extract_data, Transform
from utils import get_city_info_from_db, get_engine, establish_db_connection, close_db_connection
from pprint import pprint


# cities_info = [
#     {"id": 1, "lon": 44.8271, "lat": 41.7151},
#     {"id": 2, "lon": 41.6367, "lat": 41.6168},
# ]

conn, cur = establish_db_connection('weather')
cities_info = get_city_info_from_db(cur)
close_db_connection(conn, cur)

start_date = "2025-07-01"
end_date = "2025-07-02"
r = extract_data(start_date, end_date, cities_info)

# pprint(r)

# print(type(r[1]['hourly']['time'][0]))

transformer = Transform(r)
transformer.transform()

df = transformer.transformed_dataframe

#print(df)

engine = get_engine("weather")

df.to_sql("weather_data", engine, if_exists="replace", index=False)

# test those
conn, cur = establish_db_connection('weather') #fill arguments
info = get_city_info_from_db(cur)
close_db_connection(conn, cur)

pprint(info)