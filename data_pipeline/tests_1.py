from etl_scripts import extract_data, Transform
from pprint import pprint

cities_info = [
    {"id": 1, "lon": 44.8271, "lat": 41.7151},
    #{"id": 2, "lon": 41.6367, "lat": 41.6168},
]

start_date = "2025-07-01"
end_date = "2025-07-02"
r = extract_data(start_date, end_date, cities_info)

#pprint(r)

#print(type(r[1]['hourly']['time'][0]))

transformer = Transform(r)
transformer.transform()