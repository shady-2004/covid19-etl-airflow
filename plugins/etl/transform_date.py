import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from pathlib import Path
import os
def transform_date():

    PROJECT_ROOT = Path(__file__).parent.parent.parent  # two levels up from plugins/etl/
    
    os.chdir(PROJECT_ROOT)


    # Create database connection
    params = quote_plus(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=host.docker.internal,1433;"
    "Database=covid_wh;"
    "UID=airflow_user;"  
    "PWD=airflow_user;"  
    "TrustServerCertificate=yes;"
)

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    # Load CSV and convert to pure date
    dates = pd.read_csv('./Dates/dates.csv')
    
    # Convert date column to datetime and extract day, month, year
    dates['date'] = pd.to_datetime(dates['date']).dt.date
    dates['day'] = pd.to_datetime(dates['date']).dt.day
    dates['month'] = pd.to_datetime(dates['date']).dt.month
    dates['year'] = pd.to_datetime(dates['date']).dt.year
    # Convert date back to datetime64 for SQL compatibility
    dates['date'] = pd.to_datetime(dates['date'])

    # Load existing dates from the database
    existing_dates = pd.read_sql("SELECT date FROM dim_date", engine)
    
    # Convert existing dates to date format for comparison
    existing_dates['date'] = pd.to_datetime(existing_dates['date']).dt.date

    # Find new dates not in the database
    new_dates = dates[~dates['date'].dt.date.isin(existing_dates['date'])]

    if not new_dates.empty:
        # Remove duplicates in new_dates to be safe
        new_dates = new_dates.drop_duplicates(subset=['date'])
        new_dates.to_sql('dim_date', engine, if_exists='append', index=False, chunksize=1000)
