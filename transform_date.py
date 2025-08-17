import pandas as pd


def transform_date() :
  dates = pd.read_csv('./Dates/dates.csv')
  dates['date'] = pd.to_datetime(dates['date'])
  dates['day'] = dates['date'].dt.day
  dates['month'] = dates['date'].dt.month
  dates['year'] = dates['date'].dt.year
  dates.to_csv('./Dates/dates.csv',index = False)

transform_date()