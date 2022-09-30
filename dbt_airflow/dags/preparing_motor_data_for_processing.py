import os
import datetime

from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.ssh.hooks.ssh import SSHHook

from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta

DAG_ID = "preparing_motor_data_for_processing"
SSH_CONN_ID = "qa_ai_app1"
SOURCES_VAR_NAME = "sources_motor_data"

default_args = {
    'owner': 'jsanchez',    
    'start_date': datetime.datetime(2022, 9, 27),
    #'end_date': datetime(),
    #'depends_on_past': False,
    #'email': 'dataplatform@dynatronsoftware.com',
    #'email_on_failure': True,
    #'email_on_retry': True,
    # If a task fails, retry it once after waiting
    # at least 1 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)

def create_dag():
    start_unzip_files = DummyOperator(task_id="start_unzip_files")
    sources = Variable.get(SOURCES_VAR_NAME, default_var=[], deserialize_json=True)

    for num in range(len(sources)):
        unzip_file = SSHOperator(
            task_id=sources[num]["task_id"], 
            ssh_conn_id=SSH_CONN_ID,
            command= "cd " + sources[num]["location"] + "&& cp $(ls -tr | grep " + sources[num]["file_name"] + " | tail -n 1) ~/"
        )

        start_unzip_files >> unzip_file


globals()[DAG_ID] = create_dag()