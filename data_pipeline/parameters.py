cities_info = {
    "Tbilisi": {"id": 1, "lon": 44.8271, "lat": 41.7151},
    "Batumi": {"id": 2, "lon": 41.6367, "lat": 41.6168},
    "Zugdidi": {"id": 3, "lon": 41.8709, "lat": 42.5090},
    "Kutaisi": {"id": 4, "lon": 42.7180, "lat": 42.2480},
    "Gori": {"id": 5, "lon": 44.1083, "lat": 41.9862},
}

cities = ["Tbilisi", "Batumi", "Zugdidi", "Kutaisi", "Gori"]

historic_data_base_url = "https://archive-api.open-meteo.com/v1/archive"
forecast_data_base_url = "https://api.open-meteo.com/v1/forecast"

params = {
    "start_date": "2025-07-01",
    "end_date": "2025-07-02",
    "longitude": 44.8271,
    "latitude": 41.7151,
    "hourly": ["temperature_2m", "weather_code", "wind_speed_10m"],
    "timezone": "GMT",
    "temporal_resolution": "hourly_6",
}

weather_code_map = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}