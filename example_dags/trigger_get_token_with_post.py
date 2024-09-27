# Copyright 2024, Battelle Energy Alliance, LLC All Rights Reserved

import requests
import json
import base64

# Base64 encode the credentials
username = "admin"
password = "admin"
credentials = f"{username}:{password}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()

url = "http://localhost:8889/api/v1/dags/get_token_with_key_secret/dagRuns"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_credentials}"
}
data = {
    "conf": {
        "deeplynx_url": "https://deeplynx.azuredev.inl.gov",
        "api_key": "OWU5MDM4MTktY2MzOS00NWEzLWEzNmEtYWFiNmFkYTExMTc3",
        "api_secret": "MjcyZmJlNmEtZTM0MS00ZWU3LWFhNjAtMzk3MWM5MGNlNjc0"
    }
}

response = requests.post(url, headers=headers, data=json.dumps(data))

if response.status_code == 200:
    print("DAG triggered successfully!")
else:
    print("Failed to trigger DAG:", response.text)
