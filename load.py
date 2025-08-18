import pandas as pd
import glob
from sqlalchemy import create_engine




files = glob.glob("Transformed_data/*.csv")
dates = pd.read_csv("./Dates/dates.csv")

server = r'SHIKO\SQLEXPRESS'  # raw string to handle backslash
database = 'covid_wh'
username = 'sa'
password = 'your_password'

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

dates = pd.read_csv('./Dates/dates.csv')

dates.to_sql('dim_date',engine,if_exists ='append',index = False)

dim_date = pd.read_sql('SELECT id,date FROM dim_date',engine)

cnt = 0
for file in files :
  data = pd.read_csv(file)
  date = dates['date'][0]
  country = data['country_region']
  country.to_sql('dim_country',engine,if_exists ='append',index = False)
  dim_country = pd.read_sql('SELECT id , country FROM dim_country',engine)


