from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
import glob
from datetime import datetime
import sys
import os

from etl.extract import extract
from etl.transform_data import transform_data
from etl.transform_date import transform_date
from etl.transform_merge import transform_merge
from etl.load import load

default_args = {
  "owner" : 'airflow',
  "start_date":datetime(2025,8,19),
  'retries':1,
}

with DAG(
  dag_id = "covid19_etl_pipeline",
  default_args=default_args,
  schedule_interval='@daily'
)as dag:
  ##########TASKS################
  extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
  )

  transform_data_task = PythonOperator(
    task_id = "transform_data",
    python_callable = transform_data,
  )

  transform_date_task = PythonOperator(
    task_id = "transform_date",
    python_callable = transform_date,
  )

  transform_merge_task = PythonOperator(
    task_id = "transform_merge",
    python_callable = transform_merge,
  )

  load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )
  ##########SENSORS#############
  wait_for_dates_file = FileSensor(
    task_id = "wait_for_dates_csv",
    filepath = "Dates/dates.csv",
    fs_conn_id="fs_default",
    poke_interval = 30,
    timeout  = 60 * 10,
    mode="poke"
  )


  wait_for_fact_file = FileSensor(
    task_id = "wait_for_dates_csv",
    filepath = "fact_covid.csv",
    fs_conn_id="fs_default",
    poke_interval = 30,
    timeout  = 60 * 10,
    mode="poke"
  )

  def _check_for_files(file_name):
    files = glob.glob(f"{file_name}/*.csv")
    return len(files) > 0
  
  wait_for_extracted_data = PythonSensor(
    task_id="wait_for_extracted_data",
    python_callable=_check_for_files,
    op_args=['Extracted_data'],
    poke_interval=60, timeout=60*30,
    mode="reschedule" 
    )
  
  wait_for_transformed_data = PythonSensor(
    task_id="wait_for_transformed_data",
    python_callable=_check_for_files,
    op_args=['Transformed_data'],
    poke_interval=60, timeout=60*30,
    mode="reschedule" 
    )
  
  extract_task >> wait_for_extracted_data
  wait_for_extracted_data >> transform_data_task
  transform_data_task >> wait_for_dates_file
  wait_for_dates_file >> transform_date_task
  transform_date_task >> wait_for_dates_file >> wait_for_transformed_data >> transform_merge_task
  transform_merge_task >> wait_for_fact_file >> load_task

  