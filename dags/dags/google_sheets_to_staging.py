from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
import requests
import csv
import io
import json
import pytz

log = LoggingMixin().log

# Default arguments for the DAG
default_args = {
    'owner': 'balaji',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 10),  # Fixed date in the past
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'simple_google_sheets_test',
    default_args=default_args,
    description='Read Google Sheets via CSV export and process',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['google_sheets', 'test', 'csv'],
)

def read_google_sheet_csv(**context):
    """
    Read Google Sheets by exporting to CSV, add timestamp column,
    skip empty rows, and save to known folder.
    """
    # Get Sheet ID from Airflow Variable (fallback to default)
    sheet_id = Variable.get(
        "google_sheet_id",
        default_var="16Bt5nIVHJC9M4F-OgoQg7OTQeXANqk7gwfHVMHsVpTE"
    )
    csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0'
    
    log.info(f"Attempting to read from: {csv_url}")
    response = requests.get(csv_url)
    response.raise_for_status()

    # Current timestamp (India timezone)
    now_str = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
    
    csv_reader = csv.DictReader(io.StringIO(response.text))
    
    processed_data = []
    for row in csv_reader:
        # Skip fully empty rows
        if not any((value or "").strip() for value in row.values()):
            continue
        row["read_timestamp"] = now_str
        processed_data.append(row)

    log.info(f"Successfully read {len(processed_data)} non-empty rows from Google Sheets")

    # Ensure data folder exists inside DAGs folder
    save_dir = Path("/opt/airflow/dags/data")
    save_dir.mkdir(parents=True, exist_ok=True)

    # Save to a timestamped JSON file
    file_name = f"sheet_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    file_path = save_dir / file_name
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(processed_data, f, ensure_ascii=False)

    # Also save to a fixed "latest.json"
    latest_path = save_dir / "latest.json"
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(processed_data, f, ensure_ascii=False)

    log.info(f"Data saved to: {file_path}")
    log.info(f"Latest data also saved to: {latest_path}")
    
    return str(file_path)  # Pass path to next task

def process_data(**context):
    """
    Read from file saved in previous task and process.
    """
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='read_sheet_csv')

    if not file_path or not Path(file_path).exists():
        log.warning("No file found from previous task")
        return
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    log.info(f"Processing {len(data)} rows from file: {file_path}")
    log.info("Data processing completed - ready for staging table insert")
    return f"Processed {len(data)} rows successfully"

# Define tasks
read_task = PythonOperator(
    task_id='read_sheet_csv',
    python_callable=read_google_sheet_csv,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Task dependencies
read_task >> process_task
