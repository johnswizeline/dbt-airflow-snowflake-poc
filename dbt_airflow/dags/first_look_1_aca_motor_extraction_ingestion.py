import os
import logging
import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "first_look_1_aca_motor_extraction_ingestion"
SSH_CONN_ID = "qa_ai_app1"
POSTGRES_CONN_ID = "qa_ai_db1_external_db"

default_args = {
    'owner': 'airflow',    
    'start_date': datetime.datetime(2022, 10, 6),
    #'end_date': datetime(),
    #'depends_on_past': False,
    'email': ['dataplatform@dynatronsoftware.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    # If a task fails, retry it once after waiting
    # at least 1 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    start_extracting_files = DummyOperator(task_id="start_extracting_files")
    ### ACA Updates 8am
    step1_aca = SSHOperator(
        task_id = "aca_daily_extraction_",
        ssh_conn_id=SSH_CONN_ID,
        command= "nohup sudo /bin/bash /srv/AAIADataAcquisition/get-aaia-data.sh"
    )
    ### Full Motordata from FTP 3pm
    step2_motor = SSHOperator(
        task_id = "motor_daily_update",
        ssh_conn_id=SSH_CONN_ID,
        command= "/bin/bash /srv/MotorDataAcquisition/start.sh"
    )
    ### Perform Updates to Staging tables (after PCDB has been done)
    step1a_update_pcdb_vcdb = PostgresOperator(
        task_id="update_pcdb_vcdb",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            call pcdb_staging.updatetables();
            call vcdb_staging.updatetables();
        """,
    )
    ### Ingesting Motor Data  
    step2a_motor_load = SSHOperator(
        task_id = "motor_ingestion",
        ssh_conn_id=SSH_CONN_ID,
        command= "nohup sudo /bin/bashbash /opt/data/ingestion/poc/scripts/ingest_motor.sh"
    )
    ### Perform Updates to Staging tables (after MOTOR has been uploades)
    step1b_update_motor = PostgresOperator(
        task_id="update_motor",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call motor_staging.updatetables();",
    )

    start_extracting_files >> [step1_aca, step2_motor]
    step1_aca >> step1a_update_pcdb_vcdb
    step2_motor >> step2a_motor_load >> step1b_update_motor