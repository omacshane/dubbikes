import sys
sys.path.append('../..')
sys.path.append('..')
sys.path.append('.')

import pandas as pd
from sqlalchemy import create_engine
from src.conf.config import CONFIG


API_KEY = CONFIG['api_key']
CONTRACT_NAME = CONFIG['contract_name']
DB_NAME     = CONFIG['db_name']
SQL_DB = CONFIG['sql_path'] + DB_NAME

engine = create_engine(SQL_DB, echo=True)

def query_db_tail():

    db_engine = create_engine(SQL_DB, echo=True)

    query = pd.read_sql_query('select * from bikes', db_engine).tail()

    return query


if __name__== "__main__":

    print(query_db_tail())