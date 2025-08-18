import pandas as pd
import glob
import os
import shutil
from sqlalchemy import create_engine
from urllib.parse import quote_plus
def transform_data() : 
  
  columns = ["confirmed","deaths","recovered","active","incident_rate","case_fatality_ratio","country",'last_update']
  
  server = r'SHIKO\SQLEXPRESS'
  database = 'covid_wh'
  driver = 'ODBC Driver 17 for SQL Server'

  params = quote_plus(
    f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;"
 )

  engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


  files = glob.glob("Extracted_data/*.csv")
  folder_name1 = "Transformed_data"
  folder_name2 = "Dates"
  folder_name3 = 'Countries'


  if os.path.exists(folder_name1):
      shutil.rmtree(folder_name1)
  
  if os.path.exists(folder_name2):
      shutil.rmtree(folder_name2)
  
  if os.path.exists(folder_name3):
      shutil.rmtree(folder_name3)

  os.makedirs(folder_name1)
  os.makedirs(folder_name2)
  os.makedirs(folder_name3)


  dates = pd.DataFrame({"date":[]})

  cnt = 0
  for file in files :
      data = pd.read_csv(file)
      data.columns = data.columns.str.lower()
      data.columns = data.columns.str.replace(r'[^a-zA-Z]','_',regex=True)
      data.columns = data.columns.str.replace(r'[^a-zA-Z]+$', '', regex=True)
      data.columns = data.columns.str.replace('incidence_rate','incident_rate')
      data.rename(columns={'incidence_rate': 'incident_rate'}, inplace=True ,errors='ignore')

      data.columns = data.columns.str.replace('country_region',"country")
      # Reading date
      date = pd.to_datetime(data['last_update'][0],format='mixed').date()
      dates = pd.concat([dates, pd.DataFrame({"date": [date]})], ignore_index=True)

      # Cleaning and keeping only necessary data
      data = data[ [col for col in data.columns if col in columns] ]

      data.dropna(inplace=True)
      data.drop_duplicates(inplace=True)
      data.to_csv(f"./{folder_name1}/{cnt}_Transformed.csv",index=False)

      existing_countries  = pd.read_sql("SELECT country FROM dim_country",engine)

      new_countries = data['country'].drop_duplicates()

    # filter out ones already in DB
      to_insert = new_countries[~new_countries.isin(existing_countries['country'])]
    
      if not to_insert.empty:
        to_insert.to_frame().to_sql('dim_country', engine, if_exists='append', index=False)
      
      cnt +=1
  
  dates.to_csv(f"./{folder_name2}/dates.csv",index=False)

transform_data()
    
    

