from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from helpers import * 
import logging 
from dotenv import dotenv_values 
import tempfile 
import os 
import pandas as pd 
from sqlalchemy import create_engine

tokens = ['INJ', 'QNT', 'STORJ', 'VELO', 'SOL', 'JTO', 'ICP', 'SHIB', 'AUCTION', 'OCEAN', 'BONK', 'TIME', 'BTC', 'JUP', 'ILV']

# Initialize environment variables
env = dotenv_values(".env")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'elias_crypto_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for crypto data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the tasks
def extract_data_from_api(asset, date, **kwargs):
    data = get_token_stats(asset)
    filename = f'{asset}_{date}'
    data.to_csv(f'/tmp/{filename}.csv')
    filename = to_zipfile(f'/tmp/{filename}.csv')
    
    return filename

def save_to_s3(filename, **kwargs):
    s3 = s3_client()
    try:
        s3.upload_file(f'/tmp/{filename}', 'elias-crypto-data', f'rawdata/{filename}')
        logging.info(f'Successfully uploaded {filename} to S3')
    except Exception as e:
        logging.error(f'Error {e} occurred while uploading {filename} to S3')

def load_to_redshift(filename, **kwargs):
    s3_to_redshift(filename)

def load_to_postgres(filename, **kwargs):
    s3_to_postgres(filename)

# Define the PythonOperator tasks
date = datetime.now().strftime('%Y-%m-%d')

for asset in tokens:
    extract_task = PythonOperator(
        task_id=f'extract_{asset}',
        python_callable=extract_data_from_api,
        op_kwargs={'asset': asset, 'date': date},
        provide_context=True,
        dag=dag
    )

    s3_task = PythonOperator(
        task_id=f'save_to_s3_{asset}',
        python_callable=save_to_s3,
        op_kwargs={'filename': f'{asset}_{date}'},
        provide_context=True,
        dag=dag
    )

    redshift_task = PythonOperator(
        task_id=f'load_to_redshift_{asset}',
        python_callable=load_to_redshift,
        op_kwargs={'filename': f'{asset}_{date}'},
        provide_context=True,
        dag=dag
    )

    postgres_task = PythonOperator(
        task_id=f'load_to_postgres_{asset}',
        python_callable=load_to_postgres,
        op_kwargs={'filename': f'{asset}_{date}'},
        provide_context=True,
        dag=dag
    )

    # Define task dependencies
    extract_task >> s3_task >> [redshift_task, postgres_task]
