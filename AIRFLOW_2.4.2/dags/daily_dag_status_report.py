from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from airflow.models import DagRun # type: ignore
from datetime import datetime, timedelta
import pytz
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Adjust based on when you want to start running this DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Create the DAG object
with DAG(
    'daily_dag_status_report',  # The name of the DAG
    default_args=default_args,
    description='A DAG to generate daily status reports of DAG runs',
    schedule_interval='@daily',  # The schedule to run this DAG (daily)
    catchup=False  # Do not backfill for past dates
) as dag:

    # Python function to extract DAG run details from Airflow's metadata database
    def extract_dag_run_info():
        # Set timezone to UTC for datetime objects
        utc = pytz.UTC
        
        # Get the current date and the previous day's date with timezone awareness
        yesterday = (datetime.now(utc) - timedelta(days=1)).replace(tzinfo=utc)
        today = datetime.now(utc).replace(tzinfo=utc)

        # Query DAG runs from the Airflow metadata database for the past day
        dag_runs = DagRun.find(
            execution_date=[yesterday, today]
        )

        # Log the information about DAG runs
        for dag_run in dag_runs:
            logging.info(f"DAG: {dag_run.dag_id}, Start: {dag_run.start_date}, "
                         f"End: {dag_run.end_date}, Status: {dag_run.state}")

    # Create a task to run the Python function above
    extract_task = PythonOperator(
        task_id='extract_dag_run_info',
        python_callable=extract_dag_run_info
    )

    # Add the task to the DAG
    extract_task
