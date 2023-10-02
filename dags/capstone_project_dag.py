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

# Python Modules:
from datetime import timedelta



CLUSTER_NAME = 'demo-airflow-cluster'
REGION='us-east1'
PROJECT_ID='sodium-mountain-396818'
PYSPARK_URI='gs://dataproc-airflow-example/pyspark_job.py'

BQ_DATATSET=''


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


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
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
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    transform_movie_review_data = DataprocSubmitJobOperator(
        task_id="transform_movie_review_data", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    # LOADING:
    load_user_purchase_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_user_purchase_to_bq",
        bucket="" ,
        source_objects=[],
        destination_project_dataset_table="",
        schema_fields="",
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    load_log_review_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_log_review_to_bq",
        bucket= "",
        source_objects=[],
        destination_project_dataset_table="",
        schema_fields="",
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    load_movie_review_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_movie_review_to_bq",
        bucket= "",
        source_objects=[],
        destination_project_dataset_table="",
        schema_fields="",
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    create_table_dim_date = BigQueryOperator(
        sql="",
        task_id="create_table_dim_date",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=""
    )

    create_table_dim_devices = BigQueryOperator(
        task_id="create_table_dim_devices",
        sql="",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=""
    )

    create_table_dim_location = BigQueryOperator(
        task_id="create_table_dim_location",
        sql="",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=""
    )

    create_table_dim_os = BigQueryOperator(
        task_id="create_table_dim_os",
        sql="",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=""
    )

    create_table_fact_movie_analytics = BigQueryOperator(
        task_id="create_table_fact_movie_analytics",
        sql="",
        use_legacy_sql=False,
        allow_large_results=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=""
    )

    end = DummyOperator(task_id='end')

    # Transformation
    create_dataproc_cluster >> transform_log_review_data >> transform_movie_review_data >> delete_dataproc_cluster

    # Loading files to BigQuery
    delete_dataproc_cluster >> load_user_purchase_to_bq >> load_log_review_to_bq >> load_movie_review_to_bq

    # Create DW tables
    create_table_dim_date >> create_table_dim_devices >> create_table_dim_location >> create_table_dim_os >> create_table_fact_movie_analytics


# LOADING