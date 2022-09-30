import os
import logging
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
# from datetime import timedelta

DAG_ID = "master_part_id_table_updates_pg"
POSTGRES_CONN_ID = "qa_ai_db1_external_db"

default_args = {
    'owner': 'jsanchez',    
    'start_date': datetime.datetime(2022, 9, 22),
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
    ### Build list of vins to Decode that have not been decoded from repair orders
    step0 = PostgresOperator(
        task_id="step0_decodeundecodedvins",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_decodeundecodedvins();",
    )
    ### Run Vin decoder to process list generated above. This will send the vins through our API to decode them per the rules setup in Firstlook.
    step1 = BashOperator(
        task_id="step1_processvins",
        bash_command="for vin in `cat /opt/tmp/rpts/undecoded-vins/undecoded-vins.txt`; do echo $vin; curl 'http://ai-api.dynatron.io/api/v1/vehicle/decoded-vin?vin=$vin' done;",
    )
    ### Build data set from unprocessed repair orders into the masterpartid format.
    step2a = PostgresOperator(
        task_id="step2a_setupinsertintompfromrepairorders",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_setupinsertintompfromrepairorders(%(created_at)s);",
        parameters={"created_at": "2001-01-01::timestamp"}
    )
    ### Insert the repair order data into the MasterPartID
    step2b = PostgresOperator(
        task_id="step2b_insertintompfromrepairorders",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_insertintompfromrepairorders();",
    )
    ### Update the Longest RO Part Descriptions. Look for longer descriptions and update MasterPartID Table if necessary. (Note this is a long process)
    step3 = PostgresOperator(
        task_id="step3_updatelongestropartdescriptions",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updatelongestropartdescriptions();",
    )
    ### Insert into MasterPartID table new data from motor.
    step4 = PostgresOperator(
        task_id="step4_insertintompfrommotordata",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_insertintompfrommotordata();",
    )
    ### Update MasterPartID table from data from Motor Part Family Extended table (from motor)
    step5 = PostgresOperator(
        task_id="step5_updatempmotorpartfamilyextended",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updatempmotorpartfamilyextended();",
    )
    ### Update superseded field on MasterPartID table with data from motor.
    step6 = PostgresOperator(
        task_id="step6_updatesuperseded",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updatesuperseded();",
    )
    ### Update part price on MasterPartID Table with data from motor.
    step7 = PostgresOperator(
        task_id="step7_updatepartprice",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updatepartprice();",
    )
    ### Update the part terminology names on MasterPartID Table from acapartplus table.
    step8 = PostgresOperator(
        task_id="step8_updatepartterminologynamesfromacapartplus",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updatepartterminologynamesfromacapartplus();",
    )
    ### Update qty field on MasterPartID Table *Note this is a long running process possibly > 24 hrs
    step9 = PostgresOperator(
        task_id="step9_updateqty",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updateqty();",
    )
    ### Update missing mfg (null) on MasterPartID table.
    step10 = PostgresOperator(
        task_id="step10_updatemissingmfg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_updatemissingmfg();",
    )
    ### Create temp table to store searches for longest oepn from the originaloepn field.
    step11a = PostgresOperator(
        task_id="step11a_createtmplongestoepnorigrolongest",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_createtmplongestoepnorigrolongest();",
    )
    ### Update the orignal oepn longest field in the MasterPartID table from data created in the temp tables
    step11b = PostgresOperator(
        task_id="step11b_update_aaaaoepnorigrolongest",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_update_aaaaoepnorigrolongest();",
    )
    ### Update MasterPartID table (part terminilogy id, part terminology name) based on updates from Nodson related to specific MFGOEPNStripped records
    step12 = PostgresOperator(
        task_id="step12_updateMasterPartID",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            call etl.FLR_createtmpdatachanges();
            call etl.FLR_updatetmpdatachanges();
            call etl.FLR_insertmpdataadditions();
            call etl.FLR_updatempdataadditions();
        """,
    )
    ### Delete data from masterpartid where mfgoepnstripped field is less than 7 characters
    step13 = PostgresOperator(
        task_id="step13_delete_masterpartid_mfgoepnstripped_length_less_than_seven",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_delete_masterpartid_mfgoepnstripped_length_less_than_seven();",
    )
    ### Remove Data from masterpartid table that have bad characters ($,@)
    step14 = PostgresOperator(
        task_id="step14_delete_masterpartid_mfgoepnstripped_has_bad_characters",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            call etl.FLR_updatebadmfgoepncharacters();
            call etl.FLR_deleteduplicatebadmfgoepncharacters();
        """,
    )
    ### Update the 3 additional secondary fields added for reporting against the 3 original fields(mfgoepnstripped, 
    ### oepstripped, oepnstippedwithleading0) with data based on business rules 
    step15 = PostgresOperator(
        task_id="step15_update_secondary_fields",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            call etl.FLR_fixup_mfgoepnstripped_npn();
            call etl.FLR_build_tmpstripexclusionrules_table();
            call etl.FLR_fixup_all_toyo();
            call etl.FLR_fixup_bmw();
            call etl.FLR_fixup_chr();
            call etl.FLR_fixup_frd();
            call etl.FLR_fixup_gmc();
            call etl.FLR_fixup_hon();
            call etl.FLR_fixup_hyu();
            call etl.FLR_fixup_lan();
            call etl.FLR_fixup_maz();
            call etl.FLR_fixup_mbz();
            call etl.FLR_fixup_mit();
            call etl.FLR_fixup_nis();
            call etl.FLR_fixup_sub();
            call etl.FLR_fixup_suz();
            call etl.FLR_fixup_toy();
            call etl.FLR_fixup_vol();
            call etl.FLR_fixup_vwg();
            call etl.FLR_fixup_xxx();
        """,
    )
    ### Due to the 3 fields updated in step 15 an additional step is now needed to update the parts identified through 
    ### the part updates done against the part found earlier that was only done against the MFGOEPNStripped that didn't 
    ### have any secondary fields. Now the updates need to be applied to the MFGOEPNStripped field where those are matched.
    step16 = PostgresOperator(
        task_id="step16_FixMPV2Issue",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="call etl.FLR_FixMPV2Issue();",
    )

    step0 >> step1 >> step2a >> step2b >> step3 >> step4 >> step5 >> step6 >> step7 >> step8 >> step9 >> step10 >> step11a >> step11b >> step12 >> step13 >> step14 >> step15 >> step16