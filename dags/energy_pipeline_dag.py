from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

# Import local modules since PYTHONPATH=/opt/airflow
from db import get_engine
from pipelines.elia import run_elia_pipeline
from pipelines.energie_vlaanderen import run_vlaanderen_pipeline
from pipelines.combine_data import run_combine_pipeline
from pipelines.kaggle import run_kaggle_pipeline
from pipelines.export_csv import export_all_tables_to_csv
from pipelines.export_azure import export_master_to_azure
from pipelines.generate_chart import generate_time_series_chart

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'energy_consumption_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and combine energy data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['energy', 'lab05'],
) as dag:

    @task
    def extract_elia():
        engine = get_engine()
        run_elia_pipeline(engine)
        engine.dispose()

    @task
    def extract_vlaanderen():
        engine = get_engine()
        run_vlaanderen_pipeline(engine)
        engine.dispose()

    @task
    def extract_kaggle():
        engine = get_engine()
        run_kaggle_pipeline(engine)
        engine.dispose()

    @task
    def combine_data():
        engine = get_engine()
        run_combine_pipeline(engine)
        engine.dispose()

    @task
    def export_data():
        engine = get_engine()
        export_all_tables_to_csv(engine)
        export_master_to_azure(engine)
        engine.dispose()

    @task
    def create_chart():
        generate_time_series_chart()

    # Define task dependencies
    [extract_elia(), extract_vlaanderen(), extract_kaggle()] >> combine_data() >> export_data() >> create_chart()
