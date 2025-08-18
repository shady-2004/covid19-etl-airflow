# from sqlalchemy import create_engine
# from urllib.parse import quote_plus
# import glob
# import pandas as pd



# files = glob.glob("Transformed_data/*.csv")
# dates = pd.read_csv("./Dates/dates.csv")
# server = r'SHIKO\SQLEXPRESS'
# database = 'covid_wh'
# driver = 'ODBC Driver 17 for SQL Server'

# params = quote_plus(
#     f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;"
# )

# engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# countries = pd.read_csv('./Countries/countires.csv')
# dates = pd.read_csv('./Dates/dates.csv')
