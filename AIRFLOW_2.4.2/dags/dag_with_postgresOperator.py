from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define the default arguments
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'postgres_operations_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Task 1: Create table if not exists
    create_table = PostgresOperator(
        task_id='create_table_if_not_exists',
        postgres_conn_id='nawaz',  # Connection ID set in Airflow
        sql="""
        CREATE TABLE IF NOT EXISTS dag_run_table (
            dagid VARCHAR(255),
            dagrun VARCHAR(255),
            PRIMARY KEY (dagid, dagrun)
        );
        """
    )

    # Task 2: Delete an entry from the table
    delete_entry = PostgresOperator(
        task_id='delete_entry',
        postgres_conn_id='nawaz',
        sql="""
        DELETE FROM dag_run_table WHERE dagid = '{{ dag.dagid }}' and dagrun='{{ dag_run.conf["dagrun"] }}';
        """
    )
    
    # Task 3: Insert a new entry into the table
    insert_entry = PostgresOperator(
        task_id='insert_entry',
        postgres_conn_id='nawaz',
        sql="""
        INSERT INTO dag_run_table (dagid, dagrun)
        VALUES ('{{ dag_run.conf["dagid"] }}', '{{ dag_run.conf["dagrun"] }}');
        """
    )

    # Define the order of tasks
    create_table >> insert_entry #>> delete_entry
