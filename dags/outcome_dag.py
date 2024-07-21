from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from etl.data_ingestion import get_data
from etl.transformation import transform_data
from etl.load import load_data
from datetime import datetime

with DAG(dag_id='englishPremierLeague',
         schedule_interval='@daily',
         start_date=datetime(2024, 7, 19)
         ) as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=get_data,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True)

    extract >> transform >> load
