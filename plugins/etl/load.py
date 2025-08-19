from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd

def load() :
    

    params = quote_plus(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=host.docker.internal,1433;"
    "Database=covid_wh;"
    "UID=airflow_user;"  
    "PWD=airflow_user;"  
    "TrustServerCertificate=yes;"
)

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    data = pd.read_csv('covid_fact.csv')

    data.to_sql('covid_fact' , engine, if_exists='append', index=False)
