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
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException


@dag(
    "execute-soda-dbt_01",
    default_args={
        'email': ['mohdasdar97@gmail.com'],
        #'email_on_failure': True,
        'email_on_retry': False
    },
    schedule_interval='0 18 * * *',
    start_date=datetime(2023, 12, 20),
    tags=["snapshots", "scd", "models", "facts", "dimensions"],
    catchup=False,
    max_active_tasks=30,
    max_active_runs=3
)
def soda_dbt_execution():

    @task(
        task_id=f"execute_soda_tests",
    )
    def soda_tests(**kwargs):

        try:
            soda_tests = BashOperator(
                task_id=f"execute_soda_tests",
                bash_command=f"cd /soda && soda scan -d dvdrental_gcp -c configuration.yml raw_dvdrental",
            )
            soda_tests.execute(context=kwargs)
            logging.info(f"Successfully ran soda tests in raw_dvdrental schema")
        except Exception as e:
            logging.error(e)
            if("Task failed with exception" in str(e)):
                raise AirflowSkipException
            else:
                raise e

    @task(
        task_id=f"execute_dbt_snapshots",
    )
    def dbt_snapshots(**kwargs):

        try:
            dbt_snapshots = BashOperator(
                task_id=f"execute_dbt_bsnapshots",
                bash_command=f"cd /dbt && dbt snapshot",
            )
            dbt_snapshots.execute(context=kwargs)
            logging.info(f"Successfully ran dbt build in bv schema")
        except Exception as e:
            logging.error(e)
            raise e

    @task(
        task_id=f"execute_dbt_build_bv",
    )
    def dbt_build(**kwargs):

        try:
            dbt_build = BashOperator(
                task_id=f"execute_dbt_build_bv",
                bash_command=f"cd /dbt && dbt run --models bv",
            )
            dbt_build.execute(context=kwargs)
            logging.info(f"Successfully ran dbt build in bv schema")
        except Exception as e:
            logging.error(e)
            raise e
        
        
    soda_tests = soda_tests()
    dbt_snapshots = dbt_snapshots()
    dbt_build = dbt_build()

    soda_tests >> dbt_snapshots >> dbt_build


soda_dbt_execution()