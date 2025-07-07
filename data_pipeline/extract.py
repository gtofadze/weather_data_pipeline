import requests

def extract_data(cities_info, cities, base_url, params, start_date, end_date):

    extracted_data = {}

    for city in cities:
        id = cities_info[city]["id"]
        lon = cities_info[city]["lon"]
        lat = cities_info[city]["lat"]

        params["lon"] = lon
        params["lat"] = lat
        params["start_date"] = start_date
        params["end_date"] = end_date

        r = requests.get(base_url, params=params)
        r = r.json()

        extracted_data[id] = r

    return extracted_data
