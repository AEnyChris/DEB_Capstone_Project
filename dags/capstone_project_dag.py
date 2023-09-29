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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator



# EXTRACTION


# TRANSFORMATION

# LOADING