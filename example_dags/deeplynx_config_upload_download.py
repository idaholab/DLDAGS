# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.configuration_operator import DeepLynxConfigurationOperator
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.upload_file_operator import UploadFileOperator
from deeplynx_provider.operators.download_file_operator import DownloadFileOperator
import os

# get local data paths
dag_directory = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(dag_directory, 'data')
import_data_name = "lynx_blue.png"
import_data_path = os.path.join(data_dir, import_data_name)

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
    "connection_id": "",
    "container_id": "",
    "data_source_id": "",
    "download_file_directory": "/usr/local/airflow/logs/custom_download_directory",
}

dag = DAG(
    'deeplynx_config_upload_download',
    default_args=default_args,
    description='Demonstrates using `DeepLynxConfigurationOperator` to create a custom configuration for DeepLynx communication. Requires an existing DeepLynx container and data source, along with `connection_id`, `container_id`, and `data_source_id`.',
    schedule=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

create_config = DeepLynxConfigurationOperator(
    task_id='create_config',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    temp_folder_path=dag.params["download_file_directory"],
    dag=dag
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    dag=dag
)

upload_file = UploadFileOperator(
    task_id='upload_file',
    deeplynx_config="{{ ti.xcom_pull(task_ids='create_config', key='deeplynx_config') }}",
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id='{{ dag_run.conf["container_id"] }}',
    data_source_id='{{ dag_run.conf["data_source_id"] }}',
    file_path=import_data_path,
    dag=dag
)

download_file = DownloadFileOperator(
    task_id='download_file',
    deeplynx_config="{{ ti.xcom_pull(task_ids='create_config', key='deeplynx_config') }}",
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id='{{ dag_run.conf["container_id"] }}',
    file_id="{{ ti.xcom_pull(task_ids='upload_file', key='file_id') }}",
    dag=dag
)

create_config >> get_token >> upload_file >> download_file
