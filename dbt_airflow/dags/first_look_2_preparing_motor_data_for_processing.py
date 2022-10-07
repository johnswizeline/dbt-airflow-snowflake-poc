import datetime

from airflow.decorators import dag
from airflow.models import Variable

from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import timedelta

DAG_ID = "first_look_2_preparing_motor_data_for_processing"
SSH_CONN_ID = "qa_ai_app1"
SOURCES_VAR_NAME = "sources_motor_data"
FOLDER_REPORT = "Q3_2022" 

default_args = {
    'owner': 'airflow',    
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
    start_unziping_files = DummyOperator(task_id="start_unziping_files")
    sources = Variable.get(SOURCES_VAR_NAME, default_var=[], deserialize_json=True)

    create_quarter_folder = SSHOperator(
        task_id = "create_quarter_folder",
        ssh_conn_id=SSH_CONN_ID,
        command= """
            mkdir /opt/data/motor/keepsco/{} 
            && scp /opt/data/motor/keepsco/*.xlsx /opt/data/motor/keepsco/Current_QReports 
            && mv /opt/data/motor/keepsco/*.xlsx /opt/data/motor/keepsco/{}".format(FOLDER_REPORT, FOLDER_REPORT)
        """
    )

    for num in range(len(sources)):
        unzip_file = SSHOperator(
            task_id=sources[num]["task_id"], 
            ssh_conn_id=SSH_CONN_ID,
            command= "cd " + sources[num]["location"] + "&& unzip $(ls -tr | grep " + sources[num]["file_name"] + " | tail -n 1)"
        )

        start_unziping_files >> unzip_file >> create_quarter_folder


globals()[DAG_ID] = create_dag()