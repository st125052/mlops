import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

STUDENT_ID = os.environ["STUDENT_ID"]
LAB_DATE = os.environ["LAB_DATE"]
BUCKET_NAME = f"{STUDENT_ID}-s3-{LAB_DATE}"
RAW_KEY = "raw/weather_data.csv"
SUMMARY_KEY = "processed/weather_summary.csv"
LOCAL_RAW_PATH = "/opt/airflow/data/weather_good.csv"
LOCAL_SUMMARY_PATH = "/tmp/weather_summary.csv"

REQUIRED_COLUMNS = {"date", "city", "temperature_f", "humidity_pct", "wind_speed_mph"}


def upload_raw_csv(**kwargs):
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(LOCAL_RAW_PATH, BUCKET_NAME, RAW_KEY)
    print(f"Uploaded {LOCAL_RAW_PATH} to s3://{BUCKET_NAME}/{RAW_KEY}")


def validate_data(**kwargs):
    df = pd.read_csv(LOCAL_RAW_PATH)

    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise AirflowFailException(
            f"Schema validation failed! Missing columns: {missing_cols}"
        )

    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        raise AirflowFailException(
            f"Data quality check failed! Nulls found:\n{cols_with_nulls.to_string()}"
        )

    row_count = len(df)
    print(f"Validation passed — {row_count} rows, all columns present, no nulls.")
    kwargs["ti"].xcom_push(key="row_count", value=row_count)


def transform_data(**kwargs):
    df = pd.read_csv(LOCAL_RAW_PATH)

    def wind_chill(row):
        t, v = row["temperature_f"], row["wind_speed_mph"]
        if t <= 50 and v >= 3:
            return round(
                35.74 + 0.6215 * t - 35.75 * (v ** 0.16) + 0.4275 * t * (v ** 0.16),
                1,
            )
        return None

    df["wind_chill_f"] = df.apply(wind_chill, axis=1)

    summary = (
        df.groupby("city")
        .agg(
            avg_temp_f=("temperature_f", "mean"),
            avg_humidity_pct=("humidity_pct", "mean"),
            avg_wind_mph=("wind_speed_mph", "mean"),
            avg_wind_chill_f=("wind_chill_f", "mean"),
            record_count=("date", "count"),
        )
        .round(1)
        .reset_index()
    )

    summary.to_csv(LOCAL_SUMMARY_PATH, index=False)
    print(f"Summary computed for {len(summary)} cities")
    print(summary.to_string(index=False))


def upload_summary(**kwargs):
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(LOCAL_SUMMARY_PATH, BUCKET_NAME, SUMMARY_KEY)
    print(f"Uploaded summary to s3://{BUCKET_NAME}/{SUMMARY_KEY}")


default_args = {
    "owner": STUDENT_ID,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=f"weatherflow_{STUDENT_ID}",
    default_args=default_args,
    description="WeatherFlow: CSV to S3 to Validate to Transform to S3",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["weatherflow", "lab", STUDENT_ID],
) as dag:

    t1 = PythonOperator(task_id="upload_raw_csv", python_callable=upload_raw_csv)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t4 = PythonOperator(task_id="upload_summary", python_callable=upload_summary)

    t1 >> t2 >> t3 >> t4
