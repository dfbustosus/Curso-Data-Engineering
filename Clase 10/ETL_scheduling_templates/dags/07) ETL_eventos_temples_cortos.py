import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
# Jinja templates: https://jinja.palletsprojects.com/en/3.1.x/
###### FASE DAG #########################

dag_path = os.getcwd()     #path original.. home en Docker
print(dag_path)

dag = DAG(
    dag_id="07_templated_query_ds",
    schedule_interval="@daily",
    start_date=dt.datetime(year=2023, month=6, day=10),
    #end_date=dt.datetime(year=2023, month=6, day=22),
)

# {{ds}} = provee la fecha en formato YYY-MM-DD
# {{next_ds}}= te da la siguiente fecha valida de ejecucion en YYY-MM-DD
# Referencias: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "curl -o /opt/***/data/events.json "
        "http://events_api:80/events?"
        "start_date={{ds}}&"
        "end_date={{next_ds}}"
    ),
        dag=dag,
    )

def _calculate_stats(input_path, output_path):
    """Calcular estadisticos."""
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
        task_id="calcular_stats",
        python_callable=_calculate_stats,
        op_kwargs={
            "input_path": "{}/data/events.json".format(dag_path),
            "output_path": "{}/data/stats.csv".format(dag_path),
            },
        dag=dag,
    )

fetch_events >> calculate_stats