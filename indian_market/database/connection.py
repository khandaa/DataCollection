from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url


def create_database_engine(database_url: str) -> Engine:
    url = make_url(database_url)
    if url.drivername != "mysql+pymysql":
        raise ValueError("DATABASE_URL must use the mysql+pymysql driver")
    return create_engine(url, pool_pre_ping=True, future=True)
