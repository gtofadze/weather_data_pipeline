import requests


def extract_data(start_date, end_date, cities_info, record_type='historic'):

    base_urls = {
        "historic": "https://archive-api.open-meteo.com/v1/archive",
        "forecast": "https://api.open-meteo.com/v1/forecast",
    }

    extracted_data = {}

    for city in cities_info:
        id = city["id"]
        lon = city["lon"]
        lat = city["lat"]

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "longitude": lon,
            "latitude": lat,
            "hourly": ["temperature_2m", "weather_code", "wind_speed_10m"],
            "timezone": "GMT",
            "temporal_resolution": "hourly_6",
        }

        r = requests.get(base_urls[record_type], params=params)
        r = r.json()

        extracted_data[id] = r

    return extracted_data
