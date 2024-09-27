# Copyright 2024, Battelle Energy Alliance, LLC All Rights Reserved

from airflow import DAG
from airflow.decorators import task                             # type: ignore
from airflow.operators.python_operator import PythonOperator    # type: ignore
from airflow.configuration import conf                          # type: ignore
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator # type: ignore
from deeplynx_provider.operators.upload_file_operator import UploadFileOperator  # type: ignore
from deeplynx_provider.operators.attach_node_file_operator import AttachFileOperator  # type: ignore
from deeplynx_example.scripts.node.node import DL_Node
from deeplynx_example.scripts.data_source.data_source import Data_Source
from deeplynx_example.custom_functions.custom_deeplynx_functions import get_oauth_token
from conf.DEEPLYNX_CONF import DL_Conf
from datetime import datetime
from obspy import read, Trace
import numpy as np
import os

# Get the dags_folder location from the airflow.cfg file
dags_folder = conf.get('core', 'dags_folder')
# Define other paths relative to the dags_folder
dag_dir = os.path.join(dags_folder, 'deeplynx_example')
scripts_dir = os.path.join(dag_dir, 'scripts')
data_dir = os.path.join(dag_dir, 'data')

DEEPLYNX_URL = DL_Conf.DEEPLYNX_URL
API_KEY = DL_Conf.API_KEY
API_SECRET = DL_Conf.API_SECRET
DATASOURCE_NAME = DL_Conf.DATASOURCE_NAME
DATASOURCE_ID = DL_Conf.DATASOURCE_ID
NODE_ID = DL_Conf.NODE_ID
CONTAINER_ID = DL_Conf.CONTAINER_ID
CONN_ID = DL_Conf.CONN_ID


# Add the path to the 'scripts' directory to the Python path
import sys
sys.path.insert(0, scripts_dir)

default_args = {
    'owner': 'Spencer',
    'concurrency': 1,  # Set concurrency to 1 to allow only one active run at a time
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,  # Do not backfill previous runs when DAG is first created
}

dag = DAG(
    'mseed_to_csv_upload',
    schedule_interval=None,
    default_args=default_args,
    description='Converts an .mseed file to a .csv file',
    max_active_runs=1
)

@task
def mseed_to_csv(**kwargs):
    # Retrieve the mseed file path from DAG run configuration
    mseed_filename = kwargs['dag_run'].conf['mseed_filename']

    # Generate CSV file name dynamically from MSeed file name
    mseed_basename = os.path.basename(mseed_filename)
    csv_filename = os.path.splitext(mseed_basename)[0] + '.csv'
    csv_filepath = os.path.join(data_dir, csv_filename)

    # Read MiniSEED file and extract relevant information (metadata)
    st = read(mseed_filename)
    station_code = st[0].stats.station
    location_code = st[0].stats.location
    channel_code = st[0].stats.channel
    sampling_rate = st[0].stats.delta
    starttime = st[0].stats.starttime
    endtime = st[0].stats.endtime
    waveform_data = st[0].data # Extract waveform data

    # Write data to CSV
    with open(csv_filepath, 'w') as csv_file:
        # Write header
        csv_file.write("Time,Amplitude\n")
        time = starttime
        # Write waveform data
        for value in waveform_data:
            csv_file.write(f"{time},{value}\n")
            time += sampling_rate  # Calculate time based on sample rate

    return csv_filepath

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    conn_id=CONN_ID,
    dag=dag
)

upload_file = UploadFileOperator(
    task_id='upload_file',
    conn_id=CONN_ID,
    token='{{ ti.xcom_pull(task_ids="get_token", key="token") }}',
    container_id=CONTAINER_ID,
    data_source_id=DATASOURCE_ID,
    file_path='{{ ti.xcom_pull(task_ids="mseed_to_csv", key="return_value") }}',
    # file_metadata={"processed": True},
    dag=dag
)

attach_file = AttachFileOperator(
    task_id='attach_file',
    conn_id=CONN_ID,
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id=CONTAINER_ID,
    node=NODE_ID,
    file_id="{{ ti.xcom_pull(task_ids='upload_file', key='file_id') }}",
    dag=dag
)


with dag:
    create_file = mseed_to_csv()

[create_file >> get_token_task >> upload_file >> attach_file]
