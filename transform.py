import pandas as pd
import glob
import os
import shutil

def transform() : 

  files = glob.glob("Extracted_data/*.csv")
  folder_name1 = "Transformed_data"
  folder_name2 = "Dates"
  if os.path.exists(folder_name1):
      shutil.rmtree(folder_name1)
  
  if os.path.exists(folder_name2):
      shutil.rmtree(folder_name2)

  os.makedirs(folder_name1)
  os.makedirs(folder_name2)

  dates = pd.DataFrame({"date":[]})

  cnt = 0
  for file in files :
      data = pd.read_csv(file)
      data.columns = data.columns.str.lower()
      data.columns = data.columns.str.replace(r'[^a-zA-Z]','_',regex=True)
      data.columns = data.columns.str.replace(r'[^a-zA-Z]+$', '', regex=True)
      data.columns = data.columns.str.replace('incidence_rate','incident_rate')
      # Reading date
      date = pd.to_datetime(data['last_update'][0],format='mixed').date()
      dates = pd.concat([dates, pd.DataFrame({"date": [date]})], ignore_index=True)

      # Cleaning and keeping only necessary data
      data.drop(['fips','admin','province_state',"lat",'long',"combined_key","last_update"],axis=1,inplace=True,  errors='ignore')
      data.dropna(inplace=True)
      data.drop_duplicates(inplace=True)
      data.to_csv(f"./{folder_name1}/{cnt}_Transformed.csv")
      cnt +=1
  
  dates.to_csv(f"./{folder_name2}/dates.csv",index=False)

transform()
    
    

