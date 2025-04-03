from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define a function to print the current date and time
def print_date_time():
    print(f"Current Date and Time: {datetime.now()}")

# Define a function to print a simple message
def print_hello():
    print("Hello from your first Airflow DAG!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'start_date': datetime(2023, 10, 16),  # Start date for the DAG
    'retries': 1,  # Number of retries in case of failure
}

# Define the DAG
with DAG(
    'single_dag_example',  # DAG ID
    default_args=default_args,
    description='A simple single DAG example',
    schedule_interval='@daily',  # Runs the DAG daily
    catchup=False,  # Do not run backfill for previous runs
) as dag:

    # Define the first task to print the current date and time
    print_date_task = PythonOperator(
        task_id='print_date_time',  # Unique ID for the task
        python_callable=print_date_time,  # Function to call
    )

    # Define the second task to print a message
    print_hello_task = PythonOperator(
        task_id='print_hello_message',  # Unique ID for the task
        python_callable=print_hello,  # Function to call
    )

    # Set the task execution order
    print_date_task >> print_hello_task  # Run print_hello_task after print_date_task
