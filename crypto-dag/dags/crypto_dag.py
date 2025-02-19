import json
import logging
import os
from datetime import datetime

import ccxt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_btc_infos(ti):
    hitbtc = ccxt.hitbtc()
    ticker = hitbtc.fetch_ticker("BTC/USDT")
    now = datetime.now().timestamp()
    filename = f"{now}.json"
    with open(f"./data/{filename}", "w") as f:
        json.dump(ticker, f)
    logging.info(f"BTC dumped into {filename} 👍")
    ti.xcom_push(key="filename", value=filename)


def transform_data(ti):
    filename = ti.xcom_pull(task_ids="fetch_btc_infos", key="filename")
    f = open(f"./data/{filename}")
    data = json.load(f)
    f.close()
    data_process = {
        "value": data["symbol"],
        "datetime": data["datetime"],
        "price": data["info"]["ask"]
    }
    df = pd.DataFrame(data_process, index=[0])
    if os.path.exists("./data/btc_history.csv"):
        df.to_csv("./data/btc_history.csv", mode="a", header=False, index=False)
    else:
        df.to_csv("./data/btc_history.csv", header=True, index=False)
    logging.info("Data saved in btc_history.csv 🥳")


with DAG("crypto_dag", start_date=datetime(2022, 1, 1), schedule_interval="@hourly", catchup=False) as dag:
    fetch_btc_infos = PythonOperator(task_id="fetch_btc_infos", python_callable=fetch_btc_infos)
    transform_data = PythonOperator(task_id="transform_data", python_callable=transform_data)

    fetch_btc_infos >> transform_data
