# 🌤️ End-to-End Weather Data Pipeline

> Apache Airflow · AWS EC2 · AWS S3 · Python · Docker · Open-Meteo API

![Pipeline Status](https://img.shields.io/badge/pipeline-passing-brightgreen) ![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE?logo=apacheairflow) ![AWS](https://img.shields.io/badge/AWS-EC2%20%2B%20S3-FF9900?logo=amazonaws) ![Python](https://img.shields.io/badge/Python-3.12.4-3776AB?logo=python) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Overview

A production-grade weather data pipeline that extracts live hourly forecast data for New York City from the [Open-Meteo API](https://open-meteo.com/), transforms it into structured tabular data, and loads both raw JSON and processed CSV directly to AWS S3 — orchestrated end-to-end with Apache Airflow running in Docker on AWS EC2.

**4 runs. 4 successes. 100% success rate.**

---

## Architecture
+-----------------------------------------------------------------+
|  AWS EC2 c7i.large · Ubuntu 26.04 · Docker Compose             |
|                                                                 |
|  +----------------------------------------------------------+  |
|  |  Apache Airflow 2.9.3 DAG: weather_api_pipeline          |  |
|  |                                                          |  |
|  |  Task 1: is_weather_api_ready (HttpSensor)               |  |
|  |    Validates: HTTP 200 + hourly key + time array         |  |
|  |              |                                           |  |
|  |              v                                           |  |
|  |  Task 2: fetch_weather (PythonOperator)                  |  |
|  |    GET open-meteo.com/v1/forecast                        |  |
|  |    Raw JSON → S3 Bronze layer                            |  |
|  |    Full response → XCom                                  |  |
|  |              |                                           |  |
|  |              v                                           |  |
|  |  Task 3: transform_load_weather_data (PythonOperator)    |  |
|  |    XCom pull from fetch_weather                          |  |
|  |    Unit conversion + UTC timestamps                      |  |
|  |    Structured CSV → S3 Silver layer                      |  |
|  +----------------------------------------------------------+  |
+-----------------------------------------------------------------+
|
v
+-------------------------------------+
|  AWS S3: weatheraiflowapipipeline   |
|  weather-data/                      |
|  open_meteo_forecast_{ts}.json      | <- Bronze
|  current_weather_data_NYC{ts}.csv   | <- Silver
+-------------------------------------+
---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Apache Airflow 2.9.3 | DAG scheduling, task dependencies, retries |
| Containerization | Docker Compose | Isolated runtime — webserver, scheduler, postgres |
| Cloud Compute | AWS EC2 c7i.large | 2 vCPU / 4GB RAM Ubuntu 26.04 host |
| Cloud Storage | AWS S3 + boto3 | Raw JSON and transformed CSV landing zone |
| Database | PostgreSQL 13 | Airflow metadata and task state |
| Language | Python 3.12.4 | DAG logic, transformation, S3 upload |
| Data Processing | pandas | DataFrame construction and CSV serialisation |
| API Source | Open-Meteo API | Free forecast API — no key, no auth required |
| HTTP Validation | HttpSensor | Pre-fetch API health check with body validation |

---

## Pipeline Output

### S3 Folder Structure
s3://weatheraiflowapipipeline-yml/
└── weather-data/
├── open_meteo_forecast_20260607032805.json     <- Bronze
└── current_weather_data_NYC20260607032810.csv  <- Silver
### Transformed CSV Fields
location, temperature_celsius, temperature_fahrenheit,
humidity, wind_speed, weather_code, time_of_record

cat > ~/airflow/README.md << 'ENDOFREADME'
# 🌤️ End-to-End Weather Data Pipeline

> Apache Airflow · AWS EC2 · AWS S3 · Python · Docker · Open-Meteo API

![Pipeline Status](https://img.shields.io/badge/pipeline-passing-brightgreen) ![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE?logo=apacheairflow) ![AWS](https://img.shields.io/badge/AWS-EC2%20%2B%20S3-FF9900?logo=amazonaws) ![Python](https://img.shields.io/badge/Python-3.12.4-3776AB?logo=python) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Overview

A production-grade weather data pipeline that extracts live hourly forecast data for New York City from the [Open-Meteo API](https://open-meteo.com/), transforms it into structured tabular data, and loads both raw JSON and processed CSV directly to AWS S3 — orchestrated end-to-end with Apache Airflow running in Docker on AWS EC2.

**4 runs. 4 successes. 100% success rate.**

---

## Architecture
+-----------------------------------------------------------------+
|  AWS EC2 c7i.large · Ubuntu 26.04 · Docker Compose             |
|                                                                 |
|  +----------------------------------------------------------+  |
|  |  Apache Airflow 2.9.3 DAG: weather_api_pipeline          |  |
|  |                                                          |  |
|  |  Task 1: is_weather_api_ready (HttpSensor)               |  |
|  |    Validates: HTTP 200 + hourly key + time array         |  |
|  |              |                                           |  |
|  |              v                                           |  |
|  |  Task 2: fetch_weather (PythonOperator)                  |  |
|  |    GET open-meteo.com/v1/forecast                        |  |
|  |    Raw JSON → S3 Bronze layer                            |  |
|  |    Full response → XCom                                  |  |
|  |              |                                           |  |
|  |              v                                           |  |
|  |  Task 3: transform_load_weather_data (PythonOperator)    |  |
|  |    XCom pull from fetch_weather                          |  |
|  |    Unit conversion + UTC timestamps                      |  |
|  |    Structured CSV → S3 Silver layer                      |  |
|  +----------------------------------------------------------+  |
+-----------------------------------------------------------------+
|
v
+-------------------------------------+
|  AWS S3: weatheraiflowapipipeline   |
|  weather-data/                      |
|  open_meteo_forecast_{ts}.json      | <- Bronze
|  current_weather_data_NYC{ts}.csv   | <- Silver
+-------------------------------------+
---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Apache Airflow 2.9.3 | DAG scheduling, task dependencies, retries |
| Containerization | Docker Compose | Isolated runtime — webserver, scheduler, postgres |
| Cloud Compute | AWS EC2 c7i.large | 2 vCPU / 4GB RAM Ubuntu 26.04 host |
| Cloud Storage | AWS S3 + boto3 | Raw JSON and transformed CSV landing zone |
| Database | PostgreSQL 13 | Airflow metadata and task state |
| Language | Python 3.12.4 | DAG logic, transformation, S3 upload |
| Data Processing | pandas | DataFrame construction and CSV serialisation |
| API Source | Open-Meteo API | Free forecast API — no key, no auth required |
| HTTP Validation | HttpSensor | Pre-fetch API health check with body validation |

---

## Pipeline Output

### S3 Folder Structure
s3://weatheraiflowapipipeline-yml/
└── weather-data/
├── open_meteo_forecast_20260607032805.json     <- Bronze
└── current_weather_data_NYC20260607032810.csv  <- Silver
### Transformed CSV Fields
location, temperature_celsius, temperature_fahrenheit,
humidity, wind_speed, weather_code, time_of_record

---

## DAG Configuration

| Setting | Value |
|---------|-------|
| Schedule | @daily |
| Catchup | False |
| Retries | 2 |
| Retry delay | 5 minutes |
| Execution timeout | 2 hours |
| Email on failure | Enabled |
| Sensor poke interval | 60 seconds |
| Sensor timeout | 600 seconds |

---

## Project Structure
airflow-weather-pipeline/
├── dags/
│   └── weather_dag.py        # Main DAG — all tasks defined here
├── docker-compose.yaml       # Airflow + Postgres Docker stack (placeholder credentials)
├── .gitignore
└── README.md
---

## Local Setup

### Prerequisites
- Docker and Docker Compose installed
- AWS account with S3 bucket created
- EC2 instance with IAM role granting s3:PutObject on your bucket

### 1. Clone the repository
```bash
git clone https://github.com/kings-Plus/airflow-weather-pipeline.git
cd airflow-weather-pipeline
```

### 2. Create required folders
```bash
mkdir -p logs plugins
sudo chown -R 50000:0 logs
sudo chmod -R 775 logs
```

### 3. Update credentials in docker-compose.yaml
Replace the placeholder values with your real AWS credentials:
```yaml
AWS_ACCESS_KEY_ID: YOUR_REAL_KEY_HERE
AWS_SECRET_ACCESS_KEY: YOUR_REAL_SECRET_HERE
```

### 4. Update S3 config in weather_dag.py
```python
S3_BUCKET = "your-s3-bucket-name"
S3_REGION = "your-aws-region"
```

### 5. Start the Airflow stack
```bash
docker compose up -d
```

### 6. Add the Open-Meteo connection in Airflow UI

Navigate to http://your-ec2-ip:8080 → Admin → Connections → Add:

| Field | Value |
|-------|-------|
| Connection ID | open_meteo_api |
| Connection Type | HTTP |
| Host | https://api.open-meteo.com |
| Schema | https |
| Port | 443 |

### 7. Trigger the DAG
```bash
docker exec -it airflow-airflow-webserver-1 bash
airflow dags trigger weather_api_pipeline
```

---

## Key Engineering Decisions

**Sensor-first pattern** — HttpSensor validates API health and response body structure before every fetch, preventing downstream failures on degraded API responses.

**Single source of truth params** — WEATHER_PARAMS defined once at module level, referenced by both sensor and fetch function. One change updates both.

**XCom for inter-task data passing** — fetch_weather returns the full API response to XCom. transform_load_weather_data pulls it via task_instance.xcom_pull — the correct Airflow pattern.

**Timezone-aware datetimes** — All timestamps use datetime.fromtimestamp(ts, tz=timezone.utc) — the modern Python 3.12 replacement for the deprecated utcfromtimestamp().

**IAM role authentication** — boto3 picks up credentials from the EC2 instance IAM role. No credentials hardcoded in DAG logic.

**Dual S3 output** — Raw JSON preserved at Bronze layer for reprocessability. Silver CSV contains selected fields with converted units ready for Snowflake ingestion.

---

## Challenges Solved

| Challenge | Root Cause | Resolution |
|-----------|-----------|------------|
| HttpSensor timing out on every run | Connection Host had full path — sensor doubled it | Set Host to base domain only |
| API returning 400 Bad Request | Open-Meteo renamed relativehumidity_2m in 2024 | Updated to current field names |
| ModuleNotFoundError: providers.standard | Module added in Airflow 2.10+; container runs 2.9.3 | Changed to airflow.operators.python |
| All Airflow CLI commands crashing | Logs folder owned by root; container runs as UID 50000 | chown -R 50000:0 ./logs |
| VS Code SSH disconnecting | EC2 t2.micro RAM and Swap both at 100% | Upgraded to c7i.large |
| Python 3.12 unavailable on Ubuntu 26.04 | Too new for deadsnakes PPA | Compiled from source using make altinstall |

---

## Roadmap

- [ ] Load Silver CSV from S3 into Snowflake
- [ ] Build dbt models for daily aggregates
- [ ] Tableau dashboard on Snowflake gold layer
- [ ] Dynamic task mapping for multiple cities
- [ ] Slack webhook notifications

---

## Author

**George Alwanga** — Data Engineer & Business Analyst

- Snowflake certified · dbt Cloud certified
- 6+ years across Airflow, AWS, Python, Tableau, SQL Server
- US Navy Reserve Hospital Corpsman (E-5)

📧 alwangageorgia@gmail.com
