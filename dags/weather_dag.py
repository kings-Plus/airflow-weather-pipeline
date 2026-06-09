from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime, timezone
import requests
import json
import pandas as pd
import boto3
from io import StringIO


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': False,
    'max_retry_delay': timedelta(minutes=30),
    'email': ['alwangageorgia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'execution_timeout': timedelta(hours=2),
    'trigger_rule': 'all_success',
}

WEATHER_PARAMS = {
    "latitude": 40.71,
    "longitude": -74.01,
    "hourly": "temperature_2m,relativehumidity_2m,weathercode,windspeed_10m",
    "timezone": "auto",
}

S3_BUCKET = "weatheraiflowapipipeline-yml"
S3_REGION = "us-east-1"

def upload_to_s3(file_content, s3_key, bucket_name=S3_BUCKET):
    """Upload file to S3 bucket."""
    try:
        s3_client = boto3.client('s3', region_name=S3_REGION)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_content,
            ContentType='application/json' if s3_key.endswith('.json') else 'text/csv'
        )
        print(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise

def celsius_to_fahrenheit(temp_in_celsius):
    return float(temp_in_celsius) * 9 / 5 + 32

def transform_weather_data(task_instance):
    data = task_instance.xcom_pull(task_ids='fetch_weather')
    hourly = data.get('hourly', {})
    times = hourly.get('time', [])
    temperatures = hourly.get('temperature_2m', [])
    humidities = hourly.get('relativehumidity_2m', [])
    wind_speeds = hourly.get('windspeed_10m', [])
    weather_codes = hourly.get('weathercode', [])

    if not times or not temperatures:
        raise ValueError('Open-Meteo response is missing hourly time or temperature data')

    idx = -1
    time_of_record = datetime.fromisoformat(times[idx]).replace(tzinfo=timezone.utc)
    temperature_c = temperatures[idx]
    temperature_fahrenheit = celsius_to_fahrenheit(temperature_c)
    humidity = humidities[idx] if humidities else None
    wind_speed = wind_speeds[idx] if wind_speeds else None
    weather_code = weather_codes[idx] if weather_codes else None

    transformed_data = {
        'location': f"{WEATHER_PARAMS['latitude']},{WEATHER_PARAMS['longitude']}",
        'temperature_celsius': temperature_c,
        'temperature_fahrenheit': temperature_fahrenheit,
        'humidity': humidity,
        'wind_speed': wind_speed,
        'weather_code': weather_code,
        'time_of_record': time_of_record,
    }
    df_data = pd.DataFrame([transformed_data])

    now = datetime.now(tz=timezone.utc)
    dt_string = now.strftime('%Y%m%d%H%M%S')
    filename = f'current_weather_data_NYC{dt_string}.csv'
    
    # Convert DataFrame to CSV string and upload to S3
    csv_buffer = StringIO()
    df_data.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    
    s3_key = f"weather-data/{filename}"
    upload_to_s3(csv_content, s3_key)


def fetch_open_meteo_forecast(**kwargs):
    url = "https://api.open-meteo.com/v1/forecast"
    response = requests.get(url, params=WEATHER_PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Convert to JSON and upload to S3
    json_content = json.dumps(data, indent=2)
    now = datetime.now(tz=timezone.utc)
    dt_string = now.strftime('%Y%m%d%H%M%S')
    s3_key = f"weather-data/open_meteo_forecast_{dt_string}.json"
    upload_to_s3(json_content, s3_key)
    
    return data


with DAG(
    dag_id='weather_api_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['production', 'weather'],
) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='open_meteo_api',
        endpoint='v1/forecast',
        request_params=WEATHER_PARAMS,
        response_check=lambda response: (
            response.status_code == 200
            and 'hourly' in response.json()
            and 'time' in response.json().get('hourly', {})
        ),
        poke_interval=60,
        timeout=600,
    )

    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_open_meteo_forecast,
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_weather_data
    )

    is_weather_api_ready >> fetch_weather >> transform_load_weather_data