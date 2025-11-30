from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import pandas as pd


def get_pg_conn():
    return psycopg2.connect(
        host="airflow_clean-postgres-1",
        database="airflow",
        user="airflow",
        password="airflow"
    )


def fetch_weather_for_outlet(outlet):
    lat = outlet["latitude"]
    lon = outlet["longitude"]

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    )

    resp = requests.get(url)
    data = resp.json()

    if "hourly" not in data:
        raise Exception(f"No hourly data returned for outlet {outlet['id']}")

    df = pd.DataFrame({
        "timestamp": data["hourly"]["time"],
        "temperature_2m": data["hourly"]["temperature_2m"],
        "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
        "wind_speed_10m": data["hourly"]["wind_speed_10m"],
    })

    df["outlet_id"] = outlet["id"]
    return df


def load_weather_to_db():
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, latitude, longitude FROM raw.outlet")
    outlets = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    outlets_list = [dict(zip(colnames, row)) for row in outlets]

    all_weather = []
    for outlet in outlets_list:
        try:
            df = fetch_weather_for_outlet(outlet)
            all_weather.append(df)
        except Exception as e:
            print(f"Skipping outlet {outlet['id']} due to error: {e}")
            continue

    if not all_weather:
        raise Exception("No weather data fetched for any outlet.")

    final_df = pd.concat(all_weather, ignore_index=True)

    for _, row in final_df.iterrows():
        cur.execute("""
            INSERT INTO raw.weather (
                outlet_id,
                timestamp,
                temperature_2m,
                relative_humidity_2m,
                wind_speed_10m
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row["outlet_id"],
            row["timestamp"],
            row["temperature_2m"],
            row["relative_humidity_2m"],
            row["wind_speed_10m"]
        ))

    conn.commit()
    cur.close()
    conn.close()


default_args = {
    "owner": "naga",
    "start_date": datetime(2025, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_fetch_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=load_weather_to_db,
    )

    fetch_task

