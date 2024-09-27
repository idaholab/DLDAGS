# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
    "deeplynx_url": "https://deeplynx.azuredev.inl.gov",
    "api_key": "",
    "api_secret": ""
}

with DAG(
    'get_token_with_key_secret',
    default_args=default_args,
    description='Demonstrates obtaining a DeepLynx token using `GetOauthTokenOperator`. '
                'This DAG can use default hardcoded parameters, parameters set in the UI, '
                'or parameters passed via `dag_run.conf` in a POST request trigger.',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
) as dag:

    get_token_task = GetOauthTokenOperator(
        task_id='get_token',
        host='{{ dag_run.conf["deeplynx_url"] }}',
        api_key='{{ dag_run.conf["api_key"] }}',
        api_secret='{{ dag_run.conf["api_secret"] }}',
    )

    get_token_task
