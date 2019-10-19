import pandas as pd
import requests
import sys
import json
import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float, Boolean
from sqlalchemy import DateTime

sys.path.append('../..')
sys.path.append('..')
sys.path.append('.')
from src.conf.config import CONFIG


API_KEY = CONFIG['api_key']
CONTRACT_NAME = CONFIG['contract_name']


# Global Variables
SQLITE      = 'sqlite'
# Table Names
STATIONS    = 'stations'
BIKES       = 'bikes'
DB_NAME     = CONFIG['db_name']

SQL_DB = CONFIG['sql_path'] + DB_NAME

STATIONS_FILE = 'data/static/dublin.csv'


class MyDatabase:
    # http://docs.sqlalchemy.org/en/latest/core/engines.html
    DB_ENGINE = {
        SQLITE: 'sqlite:///{DB}'
    }

    # Main DB Connection Ref Obj
    db_engine = None

    def __init__(self, dbtype, username='', password='', dbname=''):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
            print(self.db_engine)
        else:
            print("DBType is not found in DB_ENGINE")

    def create_db_tables(self):
        metadata = MetaData()
        stations = Table(STATIONS, metadata,
                      Column('id', Integer, primary_key=True),
                      Column('number', Integer),
                      Column('name', String),
                      Column('address', Integer),
                      Column('latitude', Float),
                      Column('longitude', Float)
                      )
        bikes = Table(BIKES, metadata,
                        Column('id', Integer, primary_key=True),
                        Column('address', String),
                        Column('available_bike_stands', Integer),
                        Column('available_bikes', Integer),
                        Column('banking', Boolean),
                        Column('bike_stands', Integer),
                        Column('bonus', Boolean),
                        Column('contract_name', String),
                        Column('last_update', Integer),
                        Column('name', String),
                        Column('number', Integer),
                        Column('status', String),
                        Column('latitude', Float),
                        Column('longitude', Float),
                        Column('created_time', DateTime, default=dt.datetime.utcnow)
                        )
        try:
            metadata.create_all(self.db_engine)
            print("Tables created")
        except Exception as e:
            print("Error occurred during Table creation!")
            print(e)


def create_db():

    """
    Create db and load static file containing station info
    :return: 
    """

    dbms = MyDatabase(SQLITE, dbname=DB_NAME)
    # Create Tables
    dbms.create_db_tables()

    engine = create_engine(SQL_DB, echo=True)
    print(f"connecting to {SQL_DB}")
    conx = engine.connect()

    static = pd.read_csv(STATIONS_FILE)
    static.columns = [col.lower() for col in static.columns]
    static.to_sql(name='stations',
                  con=engine,
                  if_exists='append',
                  index=True,
                  index_label='id')

    print("DB created")


if __name__== "__main__":

    create_db()

