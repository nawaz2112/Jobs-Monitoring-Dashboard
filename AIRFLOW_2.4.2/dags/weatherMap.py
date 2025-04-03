from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def fetch_weather_data(city):
    api_key = '20c35c5a8241cc7e03440cca56dd6f0f'  # Replace with your OpenWeatherMap API key
    api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Raises an error for bad status codes
        data = response.json()  # Convert the API response to JSON format
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None

def process_data(data):
    if not data:
        return None
    return {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'weather': data['weather'][0]['description'],
        'timestamp': data['dt']
    }

def validate_data(data):
    if not data:
        return False
    required_fields = ['city', 'temperature', 'weather']
    for field in required_fields:
        if field not in data or data[field] is None:
            logging.error(f"Invalid record, missing {field}")
            return False
    return True

def store_data_in_db(processed_data):
    try:
        connection = psycopg2.connect(
            dbname='postgres',  # Replace with your database name
            user='airflow',    # Replace with your PostgreSQL username
            password='airflow', # Replace with your PostgreSQL password
            host='host.docker.internal',        # Change if needed
            port='5432'              # Default PostgreSQL port
        )
        cursor = connection.cursor()
        
        insert_query = """
        INSERT INTO weather_data (city, temperature, weather, timestamp)
        VALUES (%s, %s, %s, to_timestamp(%s))
        """
        
        cursor.execute(insert_query, (
            processed_data['city'], 
            processed_data['temperature'], 
            processed_data['weather'], 
            processed_data['timestamp']
        ))
        
        connection.commit()
        logging.info("Data stored successfully")
    except Exception as e:
        logging.error(f"Error storing data in PostgreSQL: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def main_task():
    city = "New York"  # You can parameterize this city name
    raw_data = fetch_weather_data(city)
    processed_data = process_data(raw_data)
    if validate_data(processed_data):
        store_data_in_db(processed_data)
    else:
        logging.error("Failed to validate data")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',  # Run every hour
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='fetch_and_process_weather',
        python_callable=main_task
    )

    task