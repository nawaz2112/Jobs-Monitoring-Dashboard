from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlalchemy

# Function to load transaction data from CSV
def load_transaction_data():
    # Load data from CSV
    df = pd.read_csv('/opt/airflow/data/transaction_data.csv')  # Update this path to where you save the CSV file
    return df

# Function to run fraud detection model
def run_fraud_detection():
    # Load transaction data
    df = load_transaction_data()
    
    # Simple fraud detection logic (replace with your model)
    # Example: Flag transactions with amounts greater than a threshold
    threshold = 10000  # Example threshold for fraud detection
    df['is_fraud'] = df['amount'].apply(lambda x: 1 if x > threshold else 0)
    
    # Save results to PostgreSQL
    engine = sqlalchemy.create_engine('postgresql://root:root@localhost:5432/insurance_db')
    df.to_sql('fraud_detection_results', con=engine, if_exists='replace', index=False)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fraud_detection_dag',
    default_args=default_args,
    description='A simple DAG for fraud detection',
    schedule_interval='@daily',  # Run daily
)

# Define tasks
run_detection_task = PythonOperator(
    task_id='run_fraud_detection',
    python_callable=run_fraud_detection,
    dag=dag,
)

# Set task dependencies
run_detection_task