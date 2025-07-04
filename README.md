# 🚀 Airflow Jobs Monitoring Dashboard

A lightweight Streamlit app to monitor, visualize, and report Apache Airflow DAG runs in real-time — complete with email notifications and scheduled reporting.

## 🔍 Features

- 📊 **Interactive Visualizations** (Bar, Pie, Line Charts)
- 📅 **Filter by Job Name & Date Range**
- ✅ **Summary Metrics** (Total DAGs, Runs, Successes, Failures)
- 📧 **Send Email Reports** (Manual & Auto via Scheduler)
- 🕒 **Timezone-Aware (IST Conversion)**

## 🛠️ Tech Stack

- **Streamlit**, **Plotly**, **PostgreSQL**, **psycopg2**
- **APScheduler**, **SMTP**, **pandas**, **Python 3.8+**

## 📦 Getting Started

```bash
pip install -r requirements.txt
streamlit run airflow_dashboard.py
