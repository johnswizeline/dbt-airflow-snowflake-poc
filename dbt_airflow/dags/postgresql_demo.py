import os
import logging
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import timedelta

DAG_ID = "postgres_operator_dag_example"
POSTGRES_CONN_ID = "qa_ai_db1_external_db"

def export_to_csv(copy_sql, file_name):
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)

    logging.info("Exporting query to file '%s'", file_name)
    pg_hook.copy_expert(copy_sql, filename=file_name)


default_args = {
    'owner': 'airflow',    
    'start_date': datetime.datetime(2022, 9, 21),
    #'end_date': datetime(),
    #'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 1 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_airflow_demo_schema = PostgresOperator(
        task_id="create_airflow_demo_schema",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS airflow_demo AUTHORIZATION dataplatform;",
    )
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS airflow_demo.pet (
                pet_id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                pet_type VARCHAR NOT NULL,
                birth_date DATE NOT NULL,
                OWNER VARCHAR NOT NULL);
          """,
    )
    insert_pet_table = PostgresOperator(
        task_id="insert_pet_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO airflow_demo.pet (name, pet_type, birth_date, OWNER) VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO airflow_demo.pet (name, pet_type, birth_date, OWNER) VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO airflow_demo.pet (name, pet_type, birth_date, OWNER) VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO airflow_demo.pet (name, pet_type, birth_date, OWNER) VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
          """,
    )
    export_pet_to_csv = PythonOperator(
        task_id="export_pet_to_csv",
        python_callable=export_to_csv,
        op_kwargs={
                "copy_sql": "COPY (SELECT * FROM airflow_demo.pet) TO STDOUT WITH CSV HEADER DELIMITER ','",
                "file_name": "/data/airflow_demo/pet.csv"
            }
    )

    create_airflow_demo_schema >> create_pet_table >> insert_pet_table >> export_pet_to_csv