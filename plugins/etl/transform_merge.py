from sqlalchemy import create_engine
from urllib.parse import quote_plus
import glob
import pandas as pd
from pathlib import Path
import os
def transform_merge():

    PROJECT_ROOT = Path(__file__).parent.parent.parent  # two levels up from plugins/etl/
    
    os.chdir(PROJECT_ROOT)

    files = glob.glob("Transformed_data/*.csv")

    params = quote_plus(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=host.docker.internal,1433;"
    "Database=covid_wh;"
    "UID=airflow_user;"  
    "PWD=airflow_user;"  
    "TrustServerCertificate=yes;"
)

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    # Load dimension tables
    countries = pd.read_sql_table('dim_country', engine)
    dates = pd.read_sql('SELECT id, date FROM dim_date', engine)
    dates['date'] = pd.to_datetime(dates['date']).dt.date  # normalize

    final_data = pd.DataFrame()

    fact_columns = [
        "country_id", "date_id", "confirmed", "deaths",
        "recovered", "active", "incident_rate", "case_fatality_ratio"
    ]

    for file in files:
        data = pd.read_csv(file)

        data['last_update'] = pd.to_datetime(data['last_update'], errors="coerce").dt.date

        data = data.merge(countries, on='country', how="inner")
        data.rename(columns={'id': 'country_id'}, inplace=True)

        data = data.merge(dates, left_on='last_update', right_on='date', how="inner")
        data.rename(columns={'id': 'date_id'}, inplace=True)

        data.drop(["country", "date", "last_update"], axis=1, inplace=True, errors="ignore")

        data = data.reindex(columns=fact_columns, fill_value=0)

        final_data = pd.concat([final_data, data], ignore_index=True)

    final_data.to_csv('covid_fact.csv', index=False)

transform_merge()
