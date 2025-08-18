from sqlalchemy import create_engine
from urllib.parse import quote_plus
import glob
import pandas as pd


def transform_merge( ) :
    

    files = glob.glob("Transformed_data/*.csv")
    dates = pd.read_csv("./Dates/dates.csv")
    server = r'SHIKO\SQLEXPRESS'
    database = 'covid_wh'
    driver = 'ODBC Driver 17 for SQL Server'

    params = quote_plus(
        f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    countries = pd.read_sql_table('dim_country', engine)
    dates = pd.read_sql('SELECT id , date FROM dim_date', engine)

    cnt = 0
    final_data = pd.DataFrame()

    for file in files :
        data = pd.read_csv(file)
        data['date_id'] = dates['id'][cnt]
        data = data.merge(countries , on = 'country' , how = 'left')
        data.rename(columns={'id': 'country_id'}, inplace=True)
        data.drop("country" ,axis = 1 , inplace=True)
        final_data = pd.concat([ final_data , data], ignore_index=True)
      
    final_data.to_csv('covid_fact.csv' , index=False)
transform_merge()