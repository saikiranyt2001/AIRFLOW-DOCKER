from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import logging


# -------------------------
# CONFIG (with fallback)
# -------------------------
API_URL = Variable.get(
    "API_URL",
    default_var="https://jsonplaceholder.typicode.com/posts"
)


# -------------------------
# LOGGING
# -------------------------
logger = logging.getLogger(__name__)


# -------------------------
# TASK 1: Fetch API Data
# -------------------------
def fetch_data(**context):
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        logger.info(f"Fetched {len(data)} records")

        context['ti'].xcom_push(key='raw_data', value=data)

    except Exception as e:
        logger.error(f"API Fetch Failed: {e}")
        raise


# -------------------------
# TASK 2: Validate Data
# -------------------------
def validate_data(**context):
    data = context['ti'].xcom_pull(task_ids='fetch_data', key='raw_data')

    if not data or not isinstance(data, list):
        raise ValueError("Invalid API data")

    logger.info("Data validation passed")

    context['ti'].xcom_push(key='validated_data', value=data)


# -------------------------
# TASK 3: Transform Data
# -------------------------
def transform_data(**context):
    data = context['ti'].xcom_pull(task_ids='validate_data', key='validated_data')

    if data is None:
        raise ValueError("No data received from validate_data")

    transformed = [
        {
            "id": item["id"],
            "title": item["title"].strip(),
            "body": item["body"].strip()
        }
        for item in data[:10]
    ]

    if not transformed:
        raise ValueError("Transformation resulted in empty data")

    # 🔥 IMPORTANT (ensure push happens)
    context['ti'].xcom_push(key='final_data', value=transformed)

    print(f"Transformed {len(transformed)} records")  # debug

# -------------------------
# TASK 4: Store in DB
# -------------------------
def store_data(**context):
    data = context['ti'].xcom_pull(task_ids='transform_data', key='final_data')

    if data is None:
        raise ValueError("No data received from transform_data")

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INT PRIMARY KEY,
            title TEXT,
            body TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for item in data:
        cursor.execute("""
            INSERT INTO posts (id, title, body)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET title = EXCLUDED.title,
                body = EXCLUDED.body
        """, (item['id'], item['title'], item['body']))

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("Data stored in DB")


# -------------------------
# TASK 5: Data Quality Check
# -------------------------
def data_quality_check():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM posts")
    count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if count < 5:
        raise ValueError("Data quality check failed")

    logger.info(f"Data quality passed with {count} records")


# -------------------------
# TASK 6: Notifications
# -------------------------
def notify_success():
    print("✅ Pipeline completed successfully!")


def notify_failure(context):
    print(f"❌ Pipeline failed: {context}")


# -------------------------
# DEFAULT ARGS
# -------------------------
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure
}


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="advanced_api_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["production", "api", "etl"]
) as dag:

    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    store = PythonOperator(
        task_id="store_data",
        python_callable=store_data
    )

    quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check
    )

    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success
    )

    fetch >> validate >> transform >> store >> quality >> success

import requests
from airflow.models import Variable


def send_telegram(message):
    token = Variable.get("TELEGRAM_TOKEN")
    chat_id = Variable.get("TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    response = requests.post(
        url,
        json={"chat_id": chat_id, "text": message},
        timeout=5
    )

    print(response.text)  # debug


def notify_success():
    send_telegram("✅ Pipeline completed successfully!")


def notify_failure(context):
    task = context['task_instance'].task_id
    send_telegram(f"❌ Task Failed: {task}")