import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

#Fetch claims from CSV file
def fetch_claims_from_csv(csv_file_path):
   claims = []
   with open(csv_file_path, mode='r') as file:
       csv_reader = csv.DictReader(file)
       for row in csv_reader:
           claim = {
               'claim_id': row['claim_id'],
               'policy_holder': row['policy_holder'],
               'claim_amount': float(row['claim_amount']),
               'claim_date': row['claim_date'],
               'status': row['status']
           }
           claims.append(claim)
   return claims

#Task: Process claims and prepare email notifications
def process_claims(**kwargs):
    csv_file = '/opt/airflow/data/claim_status.csv'  # Update with your CSV path
    claims_data = fetch_claims_from_csv(csv_file)
    #claims_data=[
    #    {'claim_id': 'C001', 'policy_holder': 'Alice', 'claim_amount': 1500.00, 'claim_date': '2024-01-10', 'status': 'Pending', 'email': 'alice@example.com'},
    #    {'claim_id': 'C002', 'policy_holder': 'Bob', 'claim_amount': 3000.00, 'claim_date': '2024-01-15', 'status': 'Approved', 'email': 'bob@example.com'},
    #    {'claim_id': 'C003', 'policy_holder': 'Charlie', 'claim_amount': 2500.00, 'claim_date': '2024-01-20', 'status': 'Pending', 'email': 'charlie@example.com'}]
    pending_claims = []

    for claim in claims_data:
        if claim['status'] == 'Pending':
            pending_claims.append(claim)

    # Push pending claims to XCom for use in EmailOperator
    kwargs['ti'].xcom_push(key='pending_claims', value=pending_claims)

    return 'Claims processing complete.'

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'claims_email_notifications_dag',
    default_args=default_args,
    description='Fetches claims from a CSV and sends email notifications for pending claims',
    schedule_interval="@daily",  # Set schedule interval or use None for manual run
)

# Define the Python task to process claims
process_claims_task = PythonOperator(
    task_id='process_claims_task',
    python_callable=process_claims,
    provide_context=True,
    dag=dag,
)

# Define the Email task to send notifications
def send_pending_claims_email(**kwargs):
    # Retrieve pending claims from XCom
    pending_claims = kwargs['ti'].xcom_pull(task_ids='process_claims_task', key='pending_claims')

    if not pending_claims:
        return 'No pending claims to notify.'

    # Prepare email content
    subject = "Pending Claims Notification"
    body = "The following claims are pending:\n\n"
    
    for claim in pending_claims:
        body += f"Claim ID: {claim['claim_id']}\n"
        body += f"Policy Holder: {claim['policy_holder']}\n"
        body += f"Claim Amount: {claim['claim_amount']}\n"
        body += f"Claim Date: {claim['claim_date']}\n"
        body += f"Status: {claim['status']}\n\n"
    
    return body  # Return the body for use in the EmailOperator

# Define the Email task to send notifications
send_email_task = EmailOperator(
    task_id='send_email_task',
    to='mohdnawaz2112@gmail.com',  # Update with your recipient email
    subject="Pending Claims Notification",
    html_content="{{ task_instance.xcom_pull(task_ids='process_claims_task', key='pending_claims') }}",
    dag=dag,
)

# Set task dependencies
process_claims_task >> send_email_task