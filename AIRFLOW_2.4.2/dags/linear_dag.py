from airflow import DAG #type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'my_first_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='16 10 14 10 mon',  # Runs daily
    catchup=False,
) as dag:

    # Define tasks
    task1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    # Set task dependencies
    task1 >> task2  # task1 will run before task2
