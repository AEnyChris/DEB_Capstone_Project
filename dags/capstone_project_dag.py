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
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# Python Modules:
from datetime import timedelta


PROJECT_ID='sodium-mountain-396818'
REGION='us-east1'

BQ_DATATSET='deb_capstone_dw'
GCS_STAGE_BUCKET = ""

CLUSTER_NAME = 'deb-capstone-cluster'
LOG_PYSPARK_URI='gs://dataproc-airflow-example/pyspark_job.py'
MOVIE_PYSPARK_URI='gs://dataproc-airflow-example/pyspark_job.py'

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

MOVIE_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": MOVIE_PYSPARK_URI},
}


default_args = {
    'depends_on_past': False   
}

with DAG(
    'dataproc-demo',
    default_args=default_args,
    description='capstone project to load data from csv files into datawarehouse',
    schedule_interval=None,
    start_date = days_ago(2)
) as dag:
    
    start = DummyOperator(task_id='start')

    import_user_purchase_to_gcs = PostgresToGCSOperator(
        task_id="import_user_purchase_to_gcs",
        sql="SELECT * FROM user_purchase",
        bucket= GCS_STAGE_BUCKET,
        filename="user_purchase.csv",
        export_format='csv',
        use_server_side_cursor=True
    )

    # TRANSFORMATION:
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
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

    # LOADING TO WAREHOUSE:
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
        source_objects=['log_reviews.csv'],
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
        source_objects=['movie_review.csv'],
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

    create_table_dim_date = BigQueryOperator(
        task_id="create_table_dim_date",
        sql = """
               SELECT 
                    ROW_NUMBER() OVER (ORDER BY log_date) AS id_dim_date,
                    log_date,
                    SUBSTR(log_date,7,4) AS year,
                    SUBSTR(log_date,1,2 ) AS month,
                    SUBSTR(log_date,4,2) AS day
               FROM 
                    (SELECT DISTINCT log_date FROM review_logs)
            """,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_date"
    )

    create_table_dim_devices = BigQueryOperator(
        task_id="create_table_dim_devices",
        sql="""SELECT
                    ROW_NUMBER() OVER (ORDER BY device) AS id_dim_devices,
                    device
                FROM 
                    (SELECT DISTINCT device FROM review_logs)
            """,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_devices"
    )

    create_table_dim_location = BigQueryOperator(
        task_id="create_table_dim_location",
        sql="""SELECT
                    ROW_NUMBER() OVER (ORDER BY location) AS id_dim_location,
                    location
                FROM 
                    (SELECT DISTINCT device FROM location)
            """,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_location"
    )

    create_table_dim_os = BigQueryOperator(
        task_id="create_table_dim_os",
        sql="""SELECT
                    ROW_NUMBER() OVER (ORDER BY os) AS id_dim_os,
                    os
                FROM 
                    (SELECT DISTINCT os FROM review_logs)
            """,
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.dim_os"
    )

    create_table_fact_movie_analytics = BigQueryOperator(
        task_id="create_table_fact_movie_analytics",
        sql="",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f"{PROJECT_ID}.{BQ_DATATSET}.fact_movie_analytics"
    )

    end = DummyOperator(task_id='end')

    # Transformation
    create_dataproc_cluster >> transform_log_review_data >> transform_movie_review_data >> delete_dataproc_cluster

    # Loading files to BigQuery
    delete_dataproc_cluster >> load_user_purchase_to_bq >> load_log_review_to_bq >> load_movie_review_to_bq

    # Create DW tables
    create_table_dim_date >> create_table_dim_devices >> create_table_dim_location >> create_table_dim_os >> create_table_fact_movie_analytics


# LOADING