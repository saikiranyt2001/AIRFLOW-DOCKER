from fastapi import FastAPI
import requests

app = FastAPI()

AIRFLOW_URL = "http://localhost:8080/api/v1/dags/advanced_api_pipeline/dagRuns"
USERNAME = "airflow"
PASSWORD = "airflow"

@app.post("/run-pipeline")
def run_pipeline():
    response = requests.post(
        AIRFLOW_URL,
        auth=(USERNAME, PASSWORD),
        json={"conf": {}}
    )

    return {
        "status": response.status_code,
        "response": response.json()
    }
