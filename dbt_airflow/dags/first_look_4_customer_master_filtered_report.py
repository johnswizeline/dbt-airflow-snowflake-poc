import os
import logging
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
# from datetime import timedelta

DAG_ID = "first_look_4_customer_master_filtered_report"
POSTGRES_CONN_ID = "qa_ai_db1_external_db"

default_args = {
    'owner': 'airflow',    
    'start_date': datetime.datetime(2022, 10, 7),
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
    ### Create Customer Master Report Table (unfiltered)
    step1 = PostgresOperator(
        task_id="create_customer_master_report_tbl",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.create_customer_master_report_tbl();",
    )
    ### Create indexs on customer master table Note: Steps above must be completed before this step
    step2 = PostgresOperator(
        task_id="create_customer_master_report_tbl_indexes",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE INDEX idx_custmastrpt_mfgoepnstripped ON etl.customer_master_report_tbl USING btree ("MFGOEPNStripped");
            CREATE INDEX idx_custmastrpt_parttermid ON etl.customer_master_report_tbl USING btree ("PartTerminologyID");
        """,
    )
    ### Create Customer Master Filtered Report Table Note: Steps above must be completed before this step
    step3 = PostgresOperator(
        task_id="create_customer_master_filtered_report_tbl",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.create_customer_master_filtered_report_tbl();",
    )
    ### Create indexs on Customer Master Filtered Table Note: Steps above must be completed before this step
    step4 = PostgresOperator(
        task_id="create_customer_master_filtered_report_tbl_indexes",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE INDEX idx_custmastfiltsum_mfg ON etl.customer_master_filtered_report_tbl USING btree ("MFG");
            CREATE INDEX idx_custmastfiltsum_mfgoepnstripped ON etl.customer_master_filtered_report_tbl USING btree ("MFGOEPNStripped");
            CREATE INDEX etl_customer_master_filtered_rpt_tbl_subcategorid_idx ON etl.customer_master_filtered_report_tbl USING btree ("SubCategoryID");
            CREATE INDEX customer_master_filtered_report_tbl_clientid_idx on etl.customer_master_filtered_report_tbl USING btree (roclientid);
            CREATE INDEX customer_master_filtered_report_tbl_partterminologyid_idx on etl.customer_master_filtered_report_tbl USING btree ("PartTerminologyID");
        """,
    )
    ### Stage records from customer filtered report table to temp table to remove the Alpha only mfgoepn stripped data from the report table.
    step5 = PostgresOperator(
        task_id="stage_invalid_alpha_only_into_tmpremovealphaonly_tbl",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.stage_invalid_alpha_only_into_tmpremovealphaonly_tbl();",
    )
    ### Remove records from etl.customer_master_filtered_report_tbl where the mfgoepnstripped records where alpha only.
    step6 = PostgresOperator(
        task_id="remove_alpha_records",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="delete from etl.customer_master_filtered_report_tbl where 'MFGOEPNStripped' in (select mfgoepnstripped from etl.tmpremovealphaonly);",
    )
    ### Remove records from etl.customer_master_filtered_report_tbl where the mfgoepnstripped records length is <= 7 characters long
    step6a = PostgresOperator(
        task_id="remove_alpha_records_length",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="delete from etl.customer_master_filtered_report_tbl cmfrt where length(cmfrt.'MFGOEPNStripped') <=7;",
    )
    ### Remove records from etl.customer_master_filtered_report_tbl where the Year field is < 1900
    step6b = PostgresOperator(
        task_id="remove_alpha_records_year",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="delete from etl.customer_master_filtered_report_tbl where cast('Year' as int) < 1900;",
    )
    ### Remove records from etl.customer_master_filtered_report_tbl where the TotalUnits field <= 0
    step6c = PostgresOperator(
        task_id="remove_alpha_records_units",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="delete from etl.customer_master_filtered_report_tbl cmfrt where 'TotalUnits <= 0;",
    )
    ### Create Customer Master Filtered Report File
    step7 = PostgresOperator(
        task_id="create_master_customer_filtered_report_file",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.createcustomerfilteredreport();",
    )

    step1 >> step2 >> step3 >> step4 >> step5 >> step6 >> step6a >> step6b >> step6c >> step7