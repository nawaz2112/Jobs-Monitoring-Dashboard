from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os  # For environment variables

# Database connection details
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"

# Fetch the latest 100 DAG runs
def fetch_dag_runs():
    conn = psycopg2.connect(
        host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
    )
    query = """
        SELECT dag_id, state, execution_date
        FROM dag_run
        ORDER BY execution_date DESC
        LIMIT 100;
        """
    df_dag_runs = pd.read_sql(query, conn)
    conn.close()
    return df_dag_runs

# Create HTML content for email
def create_email_body(**kwargs):
    dag_runs_df = fetch_dag_runs()

    if dag_runs_df.empty:
        return "No DAG runs found."

    html_table = dag_runs_df.to_html(index=False)

    email_body = f"""
    <html>
    <head>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }}
        </style>
    </head>
    <body>
        <h2>DAG Run Status</h2>
        {html_table}
    </body>
    </html>
   """
    # Return the email body to be used in XCom
    return email_body

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 17),
}

# Define the DAG
dag = DAG(
    'daily_dag_run_status_email',
    default_args=default_args,
    description='A DAG to send daily DAG run status email',
    schedule_interval='@daily',  # Schedule at 2:38 PM daily
    catchup=False,
)

# Task to create the email body
create_email_body_task = PythonOperator(
    task_id='create_email_body',
    python_callable=create_email_body,
    provide_context=True,  # Required to push return value to XCom
    dag=dag,
)

# Task to send the email
send_email_task = EmailOperator(
    task_id='send_dag_run_status_email',
    to="mohdnawaz2112@gmail.com",  # Recipient email address
    subject='Daily DAG Run Status',
    html_content="{{ task_instance.xcom_pull(task_ids='create_email_body') }}",  # Pull email body from XCom
    dag=dag,
)

# Set task dependencies
create_email_body_task >> send_email_task
