# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.data_query_operator import DataQueryOperator, QueryType
from deeplynx_provider.operators.data_query_with_params_operator import DataQueryWithParamsOperator, QueryType
from deeplynx_provider.operators.metatype_query_operator import MetatypeQueryOperator

#####################################

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
}

dag = DAG(
    'data_query',
    default_args=default_args,
    description='Demonstrates various ways to query metatypes, relationships, and perform a graph query using the package. Requires users to create a graph in DeepLynx and edit the DAG file to match the query bodies, parameters, and properties with their graph data.',
    schedule=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    dag=dag
)

metatype_query_body = """
{
    metatypes{
        Occurrence {
            name
            NodeId
        }
    }
}
"""

query_metatype = DataQueryOperator(
    task_id='query_metatype',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type=QueryType.METATYPE,
    query_body=metatype_query_body,
    container_id='{{ dag_run.conf["container_id"] }}',
    write_to_file=True,
    dag=dag
)

relationship_query_body = """
{
    relationships{
        decomposedBy{
            _record{
                id
                relationship_name
                origin_id
                destination_id
            }
        }
    }
}
"""

query_relationship = DataQueryOperator(
    task_id='query_relationship',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type=QueryType.RELATIONSHIP,
    query_body=relationship_query_body,
    container_id='{{ dag_run.conf["container_id"] }}',
    dag=dag
)

graph_query_body = """
{
    graph(
        root_node: "4356266"
        depth: "8"
    ){
        origin_id
        origin_properties
        origin_metatype_name
        destination_id
        destination_properties
        destination_metatype_name
        depth
        edge_direction
    }
}
"""

query_graph = DataQueryOperator(
    task_id='query_graph',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type=QueryType.GRAPH,
    query_body=graph_query_body,
    container_id='{{ dag_run.conf["container_id"] }}',
    write_to_file=False,
    dag=dag
)

# also DataQueryWithParamsOperator queries

query_metatype_with_params = DataQueryWithParamsOperator(
    task_id='query_metatype_with_params',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type=QueryType.METATYPE,
    properties={'metatype_name': 'Occurrence', 'fields_list': ['name', 'NodeId']},
    container_id='{{ dag_run.conf["container_id"] }}',
    write_to_file=True,
    dag=dag
)

query_relationship_with_params = DataQueryWithParamsOperator(
    task_id='query_relationship_with_params',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type=QueryType.RELATIONSHIP,
    properties={'relationship_name': 'decomposedBy'},
    container_id='{{ dag_run.conf["container_id"] }}',
    dag=dag
)

query_graph_with_params = DataQueryWithParamsOperator(
    task_id='query_graph_with_params',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type=QueryType.GRAPH,
    properties={'root_node': '4356266', 'depth': '8'},
    container_id='{{ dag_run.conf["container_id"] }}',
    write_to_file=False,
    dag=dag
)

another_query_metatype = MetatypeQueryOperator(
    task_id='another_query_metatype',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    metatype_name='Occurrence',
    container_id='{{ dag_run.conf["container_id"] }}',
    write_to_file=True,
    dag=dag
)

get_token >> query_metatype >> query_relationship >> query_graph
get_token >> query_metatype_with_params >> query_relationship_with_params >> query_graph_with_params
get_token >> another_query_metatype
