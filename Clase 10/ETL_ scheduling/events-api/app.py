# pip install faker
from datetime import date, datetime, timedelta
import time

from numpy import random
import pandas as pd
from faker import Faker

from flask import Flask, jsonify, request


def _generate_events(end_date):
    """Generar un dataset falso con los eventos de 30 dias antes de la fecha de fin"""
    events = pd.concat(
        [
            _generate_events_for_day(date=end_date - timedelta(days=(30 - i)))
            for i in range(30)
        ],
        axis=0,
    )
    return events


def _generate_events_for_day(date):
    """Genera eventos para un dia cualquiera """
    # Usar date como seed.
    seed = int(time.mktime(date.timetuple()))
    Faker.seed(seed)
    random_state = random.RandomState(seed)

    # Determinar cuantos usuarios y cuantos eventos tenemos.
    n_users = random_state.randint(low=50, high=100)
    n_events = random_state.randint(low=200, high=2000)

    # Generando muchos usuarios
    fake = Faker()
    users = [fake.ipv4() for _ in range(n_users)]

    return pd.DataFrame(
        {
            "user": random_state.choice(users, size=n_events, replace=True),
            "date": pd.to_datetime(date),
        }
    )


app = Flask(__name__)
app.config["events"] = _generate_events(end_date=date(year=2023, month=6, day=10))


@app.route("/events")
def events():
    start_date = _str_to_datetime(request.args.get("start_date", None))
    end_date = _str_to_datetime(request.args.get("end_date", None))

    events = app.config.get("events")

    if start_date is not None:
        events = events.loc[events["date"] >= start_date]

    if end_date is not None:
        events = events.loc[events["date"] < end_date]

    return jsonify(events.to_dict(orient="records"))


def _str_to_datetime(value):
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)

# Podemos ir a: http://localhost:80/events
# Hacer el retrieve para verificar: Invoke-WebRequest -Uri "http://localhost:80/events" -OutFile "Clase 10/ETL_ scheduling/tmp/events.json" 