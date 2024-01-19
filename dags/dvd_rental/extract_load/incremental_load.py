from dotenv import load_dotenv
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)
path=os.getcwd()+'/dags/.env'
load_dotenv(path)

from airflow import DAG

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException

TMP_DATA_PATH = os.getcwd()+'/data'
TABLES_LIST_PATH = os.getcwd()+'/dags/dvd_rental/dict/tables.json'

postgres_conn_id = "postgres_dvd"
gcp_conn_id = "gcp_dvd"


@dag(
    "dvdrental_pipeline_01",
    schedule_interval='16 5 * * *',
    start_date=datetime(2023, 12, 20),
    tags=["dvdrental_pipeline"],
    catchup=False,
    max_active_tasks=30,
    max_active_runs=3
)
def el_pipeline():
    
    if(os.path.exists(TABLES_LIST_PATH)):
        with open(TABLES_LIST_PATH, 'r') as json_file:
            tables = json.load(json_file)
    else:
        logging.info(f"Company Code JSON file not found: {TABLES_LIST_PATH}")

    for table in tables:

        @task(
            task_id=f"extract_{table['table_name']}_data",
        )
        def extract_postgres(table, postgres_conn_id, **kwargs):

            sql_query = f"""
                        SELECT * FROM {table['table_name']}
                        WHERE {table['watermark_column']} >= CAST('{kwargs['prev_ds']}' AS DATE)
                        AND {table['watermark_column']} < CAST('{kwargs['ds']}' AS DATE)
                        """
            
            logging.info(f"Executing query: {sql_query}")

            try:
                output_fp = f"{TMP_DATA_PATH}/{table['table_name']}_{kwargs['ds']}.parquet"
                pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
                df = pg_hook.get_pandas_df(sql_query)
                logging.info(f"Writing {len(df)} rows to {output_fp}")
                df.to_parquet(output_fp, index=False, compression="snappy")
                logging.info(f"Successfully saved file to parquet: {output_fp}")
            except Exception as e:
                logging.error(e)
                if("Invalid object name" in str(e)):
                    raise AirflowSkipException
                else:
                    raise e
                
            return output_fp
        

        @task( 
            task_id=f"load_{table['table_name']}_to_gcs",
        )
        def load_to_gcs(table, gcp_conn_id, **kwargs):
            
            try:
                gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
                gcs_bucket = "dvdrental_project"
                gcs_object = f"{table['table_name']}/{kwargs['ds']}/{table['table_name']}_{kwargs['ds']}.parquet"
                gcs_hook.upload(
                    bucket_name=gcs_bucket,
                    object_name=gcs_object,
                    filename=f"{TMP_DATA_PATH}/{table['table_name']}_{kwargs['ds']}.parquet",
                )
                logging.info(f"Successfully uploaded file to GCS: {gcs_object}")
            except Exception as e:
                logging.error(e)
                if("Invalid object name" in str(e)):
                    raise AirflowSkipException
                else:
                    raise e
                
            return gcs_object
        

        extract_data = extract_postgres(table, postgres_conn_id)
        load_data = load_to_gcs(table, gcp_conn_id)
    
        extract_data >> load_data
        

el_pipeline()