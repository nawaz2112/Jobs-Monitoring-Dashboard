from airflow import DAG #type: ignore
from airflow.operators.email_operator import EmailOperator #type: ignore
from datetime import datetime #type: ignore

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 22),
    'retries': 1
}

dag = DAG(
    'email_dag',  # DAG name
    default_args=default_args,
    schedule_interval=None,  # DAG schedule, can be adjusted
)

# Define the EmailOperator task
email_task = EmailOperator(
    task_id='send_email',
    to='mohdnawaz2112@gmail.com',
    subject='Test',
    html_content='Hello',
    dag=dag
)
