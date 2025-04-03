from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import psutil  # To get system metrics
from datetime import datetime, timedelta

# Define threshold limits for system monitoring
CPU_THRESHOLD = 1  # Percentage
MEMORY_THRESHOLD = 1  # Percentage
DISK_THRESHOLD = 1  # Percentage

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,  # Enable email on failure
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'system_monitoring_email_alert',
    default_args=default_args,
    description='DAG for real-time alerting based on system monitoring metrics with email alerts',
    schedule_interval=timedelta(minutes=60),  # Run every 10 minutes
    start_date=days_ago(1),
    catchup=False,
)

def check_cpu_usage(**kwargs):
    cpu_usage = psutil.cpu_percent(interval=1)
    if cpu_usage > CPU_THRESHOLD:
        return f"High CPU usage detected: {cpu_usage}%"
    return None

def check_memory_usage(**kwargs):
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.percent
    if memory_usage > MEMORY_THRESHOLD:
        return f"High memory usage detected: {memory_usage}%"
    return None

def check_disk_usage(**kwargs):
    disk_info = psutil.disk_usage('/')
    disk_usage = disk_info.percent
    if disk_usage > DISK_THRESHOLD:
        return f"High disk usage detected: {disk_usage}%"
    return None

# Task to check CPU usage
check_cpu = PythonOperator(
    task_id='check_cpu_usage',
    python_callable=check_cpu_usage,
    provide_context=True,
    dag=dag,
)

# Task to check memory usage
check_memory = PythonOperator(
    task_id='check_memory_usage',
    python_callable=check_memory_usage,
    provide_context=True,
    dag=dag,
)

# Task to check disk usage
check_disk = PythonOperator(
    task_id='check_disk_usage',
    python_callable=check_disk_usage,
    provide_context=True,
    dag=dag,
)

# Sending alert via Email
send_email_alert = EmailOperator(
    task_id='send_email_alert',
    to='mohdnawaz2112@gmail.com',
    subject='System Alert: High Resource Usage',
    html_content=""" 
    {% for task_id in ['check_cpu_usage', 'check_memory_usage', 'check_disk_usage'] %}
        {{ task_instance.xcom_pull(task_ids=task_id) }}
        <br/>
    {% endfor %}
    """,
    trigger_rule='all_done',  # Ensures alert is sent if any task reports an issue
    dag=dag,
)

# Define the task dependencies
[check_cpu, check_memory, check_disk] >> send_email_alert