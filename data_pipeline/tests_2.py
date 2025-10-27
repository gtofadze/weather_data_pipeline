from utils import get_city_info_from_db, establish_db_connection, close_db_connection
from dotenv import load_dotenv
from pprint import pprint
import os

load_dotenv()
HOST = os.getenv("HOST")
USER = os.getenv("USER")
PORT = os.getenv("PORT")
PASSWORD = os.getenv("PASSWORD")

db_config = {
    "user": USER,
    "password": PASSWORD,
    "host": HOST,
    "port": PORT
}

conn, cur = establish_db_connection(**db_config, dbname='weather') #fill arguments
info = get_city_info_from_db(cur)
close_db_connection(conn, cur)

pprint(info)