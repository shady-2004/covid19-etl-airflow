import pandas as pd
import glob
import os
import shutil
from pathlib import Path



def extract() :

    PROJECT_ROOT = Path(__file__).parent.parent.parent  # two levels up from plugins/etl/
    os.chdir(PROJECT_ROOT)  

    files = glob.glob("csse_covid_19_daily_reports/*.csv")  
    folder_name = "Extracted_data"  

    if os.path.exists(folder_name):
        shutil.rmtree(folder_name) 

    os.makedirs(folder_name)

    cnt = 0
    for file in files :
        data = pd.read_csv(file)
        data.to_csv(f"./{folder_name}/{cnt}_covid.csv",index=False)
        cnt += 1
extract()