# COVID-19 ETL with Apache Airflow (Simulation)

## Overview
This project is a simulated ETL pipeline for learning how to process daily COVID-19 data using Apache Airflow. It extracts, transforms, and loads data into a data warehouse, mimicking real-world data processing with tasks and sensors to manage dependencies.

## Project Structure
```
covid19-etl-airflow/
├─ dags/                   # Airflow DAG definitions
├─ plugins/
│  └─ etl/                 # ETL functions
├─ data/
│  ├─ raw/                 # Simulated raw data
│  ├─ processed/           # Transformed data
│  └─ warehouse/           # Data warehouse tables
├─ logs/                   # Airflow logs
├─ docker-compose.yaml     # Docker setup
├─ README.md
```

## Features
- Simulates daily COVID-19 data processing
- Extracts, cleans, and loads data into a SQL database
- Uses Airflow plugins for modular ETL
- Dockerized for easy setup
- Uses tasks and sensors to manage workflow and dependencies

## Data Source
- Sample data from [Johns Hopkins CSSE COVID-19 Daily Reports](https://github.com/CSSEGISandData/COVID-19)

## Simulation
The pipeline simulates daily data arriving in `data/raw/`, extracts it, cleans and standardizes it, and loads it into a SQL database. Airflow tasks execute ETL steps, while sensors ensure dependencies are met, mimicking real-world data delays.

## ETL Workflow
The pipeline uses **PythonOperator** tasks for ETL steps and **FileSensor**/**PythonSensor** for dependency checks:

1. **extract_task** (`PythonOperator`): Reads raw CSV files, saves to `Extracted_data/`. Used to process incoming data efficiently.
2. **wait_for_extracted_data** (`PythonSensor`): Checks for CSV files in `Extracted_data/`. Ensures extraction is complete before transformation, avoiding premature processing.
3. **transform_data_task** (`PythonOperator`): Cleans data, saves to `Transformed_data/`, updates `dim_country`. Standardizes data for consistency.
4. **wait_for_dates_file** (`FileSensor`): Waits for `Dates/dates.csv`. Ensures date metadata is ready, critical for date standardization.
5. **transform_date_task** (`PythonOperator`): Standardizes dates, updates `dim_date`. Ensures consistent date formats for the warehouse.
6. **wait_for_transformed_data** (`PythonSensor`): Checks for CSV files in `Transformed_data/`. Confirms all transformations are done before merging.
7. **transform_merge_task** (`PythonOperator`): Merges data into `covid_fact.csv`. Combines data with dimension tables for the fact table.
8. **wait_for_fact_file** (`FileSensor`): Waits for `covid_fact.csv`. Ensures the merged fact table is ready before loading.
9. **load_task** (`PythonOperator`): Loads data into the `fact_covid` table. Finalizes the ETL process by storing data in the warehouse.

<img width="1776" height="136" alt="image" src="https://github.com/user-attachments/assets/3925fd40-880b-4210-a205-4b1aee44304c" />


### Why Tasks and Sensors?
- **PythonOperator**: Used for ETL tasks (`extract`, `transform_data`, `transform_date`, `transform_merge`, `load`) to execute Python functions, allowing flexible data processing with pandas and SQLAlchemy.
- **FileSensor**: Used for `wait_for_dates_file` and `wait_for_fact_file` to monitor specific files (`dates.csv`, `covid_fact.csv`), ensuring critical files are present before proceeding.
- **PythonSensor**: Used for `wait_for_extracted_data` and `wait_for_transformed_data` to check for any CSV files in folders, providing flexibility for variable file counts in dynamic data scenarios.

**Dependencies**:
```plaintext
extract_task >> wait_for_extracted_data >> transform_data_task >> wait_for_dates_file >> transform_date_task >> wait_for_transformed_data >> transform_merge_task >> wait_for_fact_file >> load_task
```

## Getting Started
### Prerequisites
- Docker & Docker Compose
- Python 3.12+
- SQL Server with `covid_wh` database

### Installation
1. Clone the repository:
```bash
git clone https://github.com/your-username/covid19-etl-airflow.git
cd covid19-etl-airflow
```
2. Start Airflow:
```bash
docker compose up
```
> **Note**: The `docker compose up` command will pull Docker images and may use at least 2 GB of disk space.

3. Set up SQL Server database with `dim_country`, `dim_date`, and `fact_covid` tables.
4. Add sample CSVs to `csse_covid_19_daily_reports/`.
5. Access Airflow at `http://localhost:8080`.

## Usage
- Place ETL scripts in `plugins/etl/`.
- Define the DAG in `dags/` to run tasks and sensors.
- Monitor tasks in the Airflow web UI.
