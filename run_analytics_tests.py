from analytics import get_data_from_db
from pprint import pprint

from analytics import create_dfs

data = get_data_from_db("weather")

pprint(data["weather_codes"]["headers"])
pprint(data["weather_codes"]["data"])

dfs = create_dfs(data)

dfs["cities_info"].show()
