from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd

def load() :
    server = r'SHIKO\SQLEXPRESS'
    database = 'covid_wh'
    driver = 'ODBC Driver 17 for SQL Server'

    params = quote_plus(
        f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    data = pd.read_csv('covid_fact.csv')

    data.to_sql('covid_fact' , engine, if_exists='append', index=False)

load()