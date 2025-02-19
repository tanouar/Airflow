FROM apache/airflow:2.10.5

ADD requirements.txt .

RUN python3 -m pip install --upgrade pip

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt