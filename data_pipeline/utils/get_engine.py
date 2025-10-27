from sqlalchemy import create_engine
from sqlalchemy import URL

def get_engine(host, user, password, db):

    url_object = URL.create(
    "postgresql+psycopg2",
    username = user,
    password = password,
    host = host,
    database = db,
    )

    engine = create_engine(url_object)
    
    return engine