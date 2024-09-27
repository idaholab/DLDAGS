# Copyright 2024, Battelle Energy Alliance, LLC All Rights Reserved

from airflow import DAG
from airflow.decorators import task                                 # type: ignore
from airflow.contrib.sensors.file_sensor import FileSensor          # type: ignore
from airflow.operators.python_operator import PythonOperator        # type: ignore
from airflow.configuration import conf                              # type: ignore
from airflow.operators.empty import EmptyOperator                   # type: ignore
from airflow.operators.dagrun_operator import TriggerDagRunOperator # type: ignore
from conf.DEEPLYNX_CONF import DL_Conf
from deeplynx_example.custom_functions.custom_airflow_functions import trigger_dag
from datetime import datetime, timedelta
import os
import shutil

# Get the dags_folder location from the airflow.cfg file
dags_folder = conf.get('core', 'dags_folder')
# Define other paths relative to the dags_folderß
dag_dir = os.path.join(dags_folder, 'deeplynx_example')
mseed_dir = os.path.join(dags_folder, 'mseed')
move_dir = os.path.join(dag_dir, 'unprocessed_mseed')

default_args = {
    'owner': 'Spencer',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 4, 19, 27),
    'catchup': False, 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('file_sensor',
           default_args=default_args, schedule_interval=None, max_active_runs=1)

start= EmptyOperator(task_id= 'start')

# Define the sensor task
file_sensor = FileSensor(
    task_id='file_sensor_task',
    filepath="dags/mseed",
    timeout=60,
    mode="reschedule",
    poke_interval=5,  # polling interval in seconds
    soft_fail=True,
    dag=dag,
)

@task
def move_files():
    # Make sure both directories exist
    if not os.path.exists(mseed_dir):
        print(f"Source directory '{mseed_dir}' does not exist.")
        return
    if not os.path.exists(move_dir):
        print(f"Destination directory '{move_dir}' does not exist.")
        return
    
    # Get a list of all files in the source directory with .mseed extension
    files = [f for f in os.listdir(mseed_dir)]
    
    # Move each file to the destination directory
    for file in files:
        source_file = os.path.join(mseed_dir, file)
        destination_file = os.path.join(move_dir, file)
        shutil.move(source_file, destination_file)
        print(f"Moved '{file}' to '{move_dir}'")
    
    new_files = [f"{move_dir}/{f}" for f in os.listdir(move_dir) if f.endswith('.mseed')]

    # Pushing new file names to XCom
    return new_files

@task
def start_processing(mseed_files):
    airflow_url = "http://host.docker.internal:8888/"  # Airflow web interface URL
    username = "<username>"                # Airflow username
    password = "<password>"                # Airflow password

    for file_name in mseed_files:
        conf = {"mseed_filename":file_name}
        trigger_dag("mseed_to_csv_upload", airflow_url, username, password, conf)

with dag:
    files = move_files()
    start_processing_task = start_processing(files)

# Set up task dependencies
[start >> file_sensor >> files >> start_processing_task]
