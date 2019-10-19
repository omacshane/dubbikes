import pandas as pd
import requests
import sys

from sqlalchemy import create_engine

sys.path.append('../..')
sys.path.append('..')
sys.path.append('.')
from src.conf.config import CONFIG


API_KEY = CONFIG['api_key']
CONTRACT_NAME = CONFIG['contract_name']
DB_NAME     = CONFIG['db_name']
SQL_DB = CONFIG['sql_path'] + DB_NAME



def get_station_data(sql_engine):

    stations_request = requests.get(f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT_NAME}",
                                    params={'apiKey': API_KEY},
                                    headers={'Accept': 'application/json'})

    stations_df = pd.read_json(stations_request.content)

    stations_df['latitude'] = stations_df['position'].apply(lambda x: x['lat'])
    stations_df['longitude'] = stations_df['position'].apply(lambda x: x['lng'])
    stations_df.drop(columns='position', inplace=True)

    try:
        stations_df.to_sql(name='bikes',
                           con=sql_engine,
                           if_exists='append',
                           index=False)
    except:
        print("Error writing to database")
    else:
        print("Successfuly wrote to database")

def main():

    engine = create_engine(SQL_DB, echo=False)

    get_station_data(engine)


if __name__== "__main__":

    main()



