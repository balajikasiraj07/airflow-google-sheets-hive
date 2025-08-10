from datetime import datetime, timedelta
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="google_sheets_to_hive",
    default_args=default_args,
    description="Read Google Sheet → save JSON → process data",
    schedule_interval=None,
    start_date=datetime(2024, 8, 1),
    catchup=False,
) as dag:

    def read_google_sheet(**context):
        """
        Reads Google Sheets, saves as latest.json, and pushes filepath to XCom.
        Logs top 5 rows for debugging.
        """

        # Google Sheets API auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_json = Variable.get("GOOGLE_SHEETS_CREDENTIALS_JSON")  # Store creds in Airflow Variables
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        # Get Sheet
        spreadsheet_id = Variable.get("GOOGLE_SHEET_ID")
        worksheet_name = Variable.get("GOOGLE_SHEET_WORKSHEET")
        sheet = client.open_by_key(spreadsheet_id).worksheet(worksheet_name)

        # Get data
        data = sheet.get_all_records()

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Remove empty rows
        df = df.dropna(how="all")

        # Add timestamp column
        df["read_timestamp"] = datetime.utcnow().isoformat()

        # Log top 5 rows
        print("=== TOP 5 ROWS READ FROM GOOGLE SHEET ===")
        print(df.head(5).to_string(index=False))
        print("=========================================")

        # Save to latest.json
        output_path = os.path.join(os.path.expanduser("~"), "latest.json")
        df.to_json(output_path, orient="records", lines=False)

        # Push file path to XCom
        context["ti"].xcom_push(key="latest_json_path", value=output_path)

    def process_data(**context):
        """
        Reads JSON file from XCom and processes the data.
        """
        file_path = context["ti"].xcom_pull(task_ids="read_google_sheet", key="latest_json_path")

        with open(file_path, "r") as f:
            records = json.load(f)

        df = pd.DataFrame(records)

        # Just a placeholder processing step
        print(f"Processing {len(df)} records...")
        print(df.head(5).to_string(index=False))

    read_google_sheet_task = PythonOperator(
        task_id="read_google_sheet",
        python_callable=read_google_sheet,
        provide_context=True,
    )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        provide_context=True,
    )

    read_google_sheet_task >> process_data_task
