# Copyright 2024, Battelle Energy Alliance, LLC All Rights Reserved

from airflow.plugins_manager import AirflowPlugin                           #type: ignore
from airflow.www.app import csrf                                            #type: ignore
from airflow.models import Variable                                         #type: ignore
from dags.conf.DEEPLYNX_CONF import DL_Conf
from dags.deeplynx_example.custom_functions.custom_deeplynx_functions import get_oauth_token
from flask import Blueprint, jsonify, request
from dags.deeplynx_example.custom_functions.custom_airflow_functions import trigger_dag
import logging
import requests
import json
from flask import request

DEEPLYNX_URL = DL_Conf.DEEPLYNX_URL
API_KEY = DL_Conf.API_KEY
API_SECRET = DL_Conf.API_SECRET

# AIRFLOW_URL gets set when endpoint is triggered
AIRFLOW_URL = ""

# Define a Flask blueprint for your custom endpoint
endpoint_bp = Blueprint("my_plugin", __name__)

@endpoint_bp.route("/api/v1/process_file", methods=["POST"])
@csrf.exempt
def process_file():

    # Get airflow url and set the global variable for use in the trigger_processing_DAG function
    global AIRFLOW_URL
    base_url = request.url_root
    AIRFLOW_URL = base_url.replace("localhost", "host.docker.internal")


    try:
        # Extract data from the POST request
        data = request.json

        # Ensure 'event' field is present and not empty
        if 'event' not in data:
            return jsonify({"error": "Missing 'event' field"}), 400
        
        event = data["event"]

        # Ensure required fields are present and not empty strings
        required_fields = ["fileID", "nodeID", "containerID", "dataSourceID"]
        for field in required_fields:
            if field not in event or not event[field].strip():
                return jsonify({"error": f"Missing or empty '{field}' field in 'event'"}), 333

        # Process the data
        result = trigger_processing_DAG(data)
        Variable.set(key="process_result", value=result)
        
        # Return a response
        return jsonify({"result": result}), 200
    
    # Handle errors
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


# Define a function to process the data
def trigger_processing_DAG(data):
    try:
        # Get event from data
        event = data["event"]

        # Extract required data
        file_id = event["fileID"]
        node_id = event["nodeID"]
        container_id = event["containerID"]
        datasource_id = event["dataSourceID"]

        # Proceed with processing the data
        params = {
            "file_id": file_id,
            "node_id": node_id,
            "container_id": container_id,
            "datasource_id": datasource_id,
            "deeplynx_url": DEEPLYNX_URL,
            "API_key": API_KEY,
            "API_secret": API_SECRET
        }

        token = get_oauth_token(DEEPLYNX_URL, API_KEY, API_SECRET, "1 minute")

        # Info to pass to get file API
        url = f"{DEEPLYNX_URL}containers/{container_id}/files/{file_id}"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
        }
        try:
            # Send API request
            response = requests.request("GET", url, headers=headers)

            if response.status_code == 200:
                # Parse JSON response
                data = response.json()

                try:
                    # Check if file has been processed
                    processed = (str(data['value']['metadata']['processed']).lower() == 'true')
                except:
                    processed = False

                if not processed:
                    # Trigger dag to process file
                    status = trigger_dag("proccess_main_file", AIRFLOW_URL, "<username>", "<password>", params)


                return "File processing DAG triggered"

            else:
                return jsonify({"error": f"Failed to retrieve file: {response.status_code}"}), response.status_code
            

        except Exception as e:
            return jsonify({"error": f"File upload failed: {e}"}), 500

    except Exception as e:
        return jsonify({"error": f"Error processing data: {e}"}), 500


# Define a custom plugin class to register the blueprint
class CustomPlugin(AirflowPlugin):
    """Custom plugin class"""

    name = "custom_plugin"
    flask_blueprints = [endpoint_bp]
