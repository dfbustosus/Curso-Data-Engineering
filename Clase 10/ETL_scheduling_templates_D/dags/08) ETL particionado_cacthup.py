import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import os

dag_path = os.getcwd()     #path original.. home en Docker

dag = DAG(
    dag_id="08_templated_path",
    schedule_interval="@daily",
    start_date=dt.datetime(year=2023, month=7, day=1),
    end_date=dt.datetime(year=2023, month=7, day=7),
    #catchup=False
)

def fetch_events(ds, **kwargs):
    start_date = ds
    end_date = kwargs['macros'].ds_add(ds, 1)
    url = f"http://events_api:80/events?start_date={start_date}&end_date={end_date}"
    response = requests.get(url)
    print(response)
    file_path = os.path.join(dag_path, 'data', 'events', f"{ds}.json")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(response.text)

fetch_events_task = PythonOperator(
    task_id="fetch_events",
    python_callable=fetch_events,
    provide_context=True,
    dag=dag
)


def _calculate_stats(ds):
    """Calculates event statistics."""
    input_path = os.path.join(dag_path, 'data', 'events', f"{ds}.json")
    output_path = os.path.join(dag_path, 'data', 'stats', f"{ds}.csv")

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    provide_context=True,
    # Required in Airflow 1.10 to access templates_dict, deprecated in Airflow 2+.
    # provide_context=True,
    dag=dag,
)


fetch_events_task >> calculate_stats