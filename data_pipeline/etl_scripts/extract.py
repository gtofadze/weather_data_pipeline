import requests


def extract_data(start_date, end_date, cities_info, record_type=1):

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "longitude": None,
        "latitude": None,
        "hourly": ["temperature_2m", "weather_code", "wind_speed_10m"],
        "timezone": "GMT",
        "temporal_resolution": "hourly_6",
    }

    historic_data_base_url = "https://archive-api.open-meteo.com/v1/archive"
    forecast_data_base_url = "https://api.open-meteo.com/v1/forecast"

    if record_type == 1:
        base_url = historic_data_base_url
    elif record_type == 2:
        base_url = forecast_data_base_url
    else:
        raise ValueError('record_type must be either 1 or 2')

    extracted_data = {}

    for city in cities_info:
        id = city["id"]
        lon = city["lon"]
        lat = city["lat"]

        params["lon"] = lon
        params["lat"] = lat
        params["start_date"] = start_date
        params["end_date"] = end_date

        r = requests.get(base_url, params=params)
        r = r.json()

        extracted_data[id] = r

    return extracted_data
