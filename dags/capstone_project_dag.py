"""
    Wizeline DEB Capstone Project


    Author: Enyone Christian Achobe

    Description:
        This project is a data pipeline workflow that ingest user data to populate
        a datawarehouse used for analytics
"""


# MODULE IMPORTATIONS:

# Airflow modules
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Python Modules:
import os
from datetime import timedelta


#------------------------------PARAMETER DEFINITIONS---------------------

# Connection Parameters
PROJECT_ID='sodium-mountain-396818'
REGION='us-east1'
GCP_CONN_ID = 'google_cloud_default'
POSTGRES_CONN_ID = 'postgres_conn_id'

# Storage and files
GCS_DATA_BUCKET_NAME = f"{PROJECT_ID}-data-bucket"
GCS_STAGE_BUCKET = f"gs://{PROJECT_ID}-stage-bucket"
SCRIPTS_BUCKET_URL = f"gs://{PROJECT_ID}-scripts-bucket"
USER_PURCHASE_FILE = "user_purchase_new.csv"
BQ_DATATSET='deb_capstone_dw'

# SQL Queries file for creating tables in BQ
# FACT_SQL_URI = f"{SCRIPTS_BUCKET_URL}/create_fact_table.sql"
# DIM_DATE_SQL_URI = f"{SCRIPTS_BUCKET_URL}/create_dim_date.sql"
# DIM_DEVICE_SQL_URI = f"{SCRIPTS_BUCKET_URL}/create_dim_device.sql"
# DIM_OS_SQL_URI = f"{SCRIPTS_BUCKET_URL}/create_dim_os.sql"
# DIM_LOCATION_SQL_URI = f"{SCRIPTS_BUCKET_URL}/create_dim_location.sql"
# USER_PURCHASE_CREATE_URL = f"{SCRIPTS_BUCKET_URL}/create_user_purchase_table.sql

DIM_DEVICE_SQL_URI = "sql_queries/create_dim_device_table.sql"
DIM_LOCATION_SQL_URI = "sql_queries/create_dim_location.sql"
DIM_OS_SQL_URI = "sql_queries/create_dim_os.sql"
FACT_SQL_URI  = "sql_queries/create_fact_table.sql"
DIM_DATE_SQL_URI = "sql_queries/create_table_dim_date.sql"
USER_PURCHASE_CREATE_URL  = "sql_queries/create_user_purchase_table.sql"
IMPORT_USER_PURCHASE_SQL_PATH= "sql_queries/import_user_purchase.sql"



# DATAPROC parameters
CLUSTER_NAME = 'deb-capstone-cluster'
LOG_PYSPARK_URI=f"{SCRIPTS_BUCKET_URL}/log_review_trans.py"
MOVIE_PYSPARK_URI=f"{SCRIPTS_BUCKET_URL}/movie_review_transf.py"
USER_PURCHASE_PYSPARK_URI = f"{SCRIPTS_BUCKET_URL}/user_purchase_process.py"


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    }
}

LOG_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": LOG_PYSPARK_URI},
}

USER_PURCHASE_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": USER_PURCHASE_PYSPARK_URI},
}

MOVIE_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": MOVIE_PYSPARK_URI},
}


# Python function for the loading of user_purchase file into CloudSQL
def ingest_data_from_gcs(
    gcs_bucket: str,
    gcs_object: str,
    postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_conn_id",
):
    """Ingest data from an GCS bucket into a postgres table.

    Args:
        gcs_bucket (str): Name of the bucket.
        gcs_object (str): Name of the object.
        postgres_table (str): Name of the postgres table.
        gcp_conn_id (str): Name of the Google Cloud connection ID.
        postgres_conn_id (str): Name of the postgres connection ID.
    """
    import tempfile

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)

    with tempfile.NamedTemporaryFile() as tmp:
        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=tmp.name
        )
        psql_hook.copy_expert(f"COPY {postgres_table} FROM stdin DELIMITER ',' CSV HEADER;", filename=tmp.name)

    return 1

# DAG parameters
default_args = {
    'depends_on_past': False,
    'email': 'ecjachobe@gmail.com',
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)   
}

DAG_ID = os.path.splitext(os.path.basename(__file__))[0]

#---------------------------------------------------------------------------------


#-------------------------------DAG INITIALIZATION----------------------------------

with DAG(
    DAG_ID,
    default_args=default_args,
    description='capstone project to load data from csv files into datawarehouse',
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False
) as dag:   
#----------------------------------------------------------------------------------------  

#--------------------------------TASKS DEFINITIONS--------------------------------------------------   

    start = DummyOperator(task_id='start')

    # #################### EXTRACTION ####################
    create_user_purchase_table = PostgresOperator(
        task_id = "create_user_purchase_table",
        sql = USER_PURCHASE_CREATE_URL,
        postgres_conn_id = POSTGRES_CONN_ID,
    )

    ingest_data_to_user_purchase_table = PythonOperator(
        task_id="ingest_data_to_user_purchase_table",
        python_callable= ingest_data_from_gcs,
        op_kwargs = {
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_DATA_BUCKET_NAME,
            "gcs_object": USER_PURCHASE_FILE,
            "postgres_table": 'user_purchase',
        }
    )

    import_user_purchase_to_gcs = PostgresToGCSOperator(
        task_id="import_user_purchase_to_gcs",
        sql=IMPORT_USER_PURCHASE_SQL_PATH,
        bucket= GCS_STAGE_BUCKET,
        filename="user_purchase.csv",
        export_format='csv',
        use_server_side_cursor=True
    )

    ################ TRANSFORMATIONS ##################
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    process_user_purchase_data = DataprocSubmitJobOperator(
        task_id="process_user_purchase_data", 
        job=USER_PURCHASE_PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    transform_log_review_data = DataprocSubmitJobOperator(
        task_id="transform_log_review_data", 
        job=LOG_PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    transform_movie_review_data = DataprocSubmitJobOperator(
        task_id="transform_movie_review_data", 
        job=MOVIE_PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    ################### LOADING DATA TO WAREHOUSE:###################
    load_user_purchase_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_user_purchase_to_bq",
        bucket= GCS_STAGE_BUCKET,
        source_objects=['user_purchase.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.user_purchase",
        schema_fields=[
                        {'name': 'invoice_number', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'stock_code', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'detail', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'quantity', 'type': 'INT64', 'mode': 'NULLABLE'},
                        {'name': 'invoice_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                        {'name': 'unit_price', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                        {'name': 'customer_id', 'type': 'INT64', 'mode': 'NULLABLE'},
                        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    load_log_review_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_log_review_to_bq",
        bucket= GCS_STAGE_BUCKET,
        source_objects=['log_review_transformed.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.review_logs",
        schema_fields=[
                        {'name': 'log_id', 'type': 'INT64', 'mode': 'NULLABLE'},
                        {'name': 'log_date', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'device', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'ipaddress', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    load_movie_review_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_movie_review_to_bq",
        bucket= GCS_STAGE_BUCKET,
        source_objects=['movie_review_transformed.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.classified_movie_review",
        schema_fields=[
                        {'name': 'customer_id', 'type': 'INT64', 'mode': 'NULLABLE'},
                        {'name': 'is_positive', 'type': 'INT64', 'mode': 'NULLABLE'},
                        {'name': 'review_id', 'type': 'INT64', 'mode': 'NULLABLE'},
                        {'name': 'insert_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    continue_process = DummyOperator(task_id="continue_process")


    ##################### CREATING DIM AND FACT TABLES IN DWH BIGQUERY ##########################
    create_table_dim_date = BigQueryOperator(
        task_id="create_table_dim_date",
        sql =f"{DIM_DATE_SQL_URI}",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_date"
    )

    create_table_dim_devices = BigQueryOperator(
        task_id="create_table_dim_devices",
        sql=DIM_DEVICE_SQL_URI,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_devices"
    )

    create_table_dim_location = BigQueryOperator(
        task_id="create_table_dim_location",
        sql=DIM_LOCATION_SQL_URI,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_location"
    )

    create_table_dim_os = BigQueryOperator(
        task_id="create_table_dim_os",
        sql=DIM_OS_SQL_URI,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_os"
    )

    create_table_fact_movie_analytics = BigQueryOperator(
        task_id="create_table_fact_movie_analytics",
        sql=FACT_SQL_URI,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.fact_movie_analytics"
    )

    end = DummyOperator(task_id='end')



    start >> create_user_purchase_table >> create_dataproc_cluster

    (
        create_dataproc_cluster 
        >> [process_user_purchase_data, transform_log_review_data, transform_movie_review_data] 
        >> delete_dataproc_cluster
        >> ingest_data_to_user_purchase_table
    )

    ingest_data_to_user_purchase_table >> import_user_purchase_to_gcs >> [load_user_purchase_to_bq, load_log_review_to_bq, load_movie_review_to_bq]

    (
        [load_user_purchase_to_bq, load_log_review_to_bq, load_movie_review_to_bq] 
        >> continue_process >> [create_table_dim_date, create_table_dim_devices, create_table_dim_location, create_table_dim_os]
        >> create_table_fact_movie_analytics 
        >> end
    )