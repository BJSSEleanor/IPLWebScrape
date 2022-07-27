"""DAG definition for extract and transform pipeline.
"""
# pylint: disable=pointless-statement
from ipaddress import ip_address
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

from src.extract import extract
from src.transform import transform


dag = DAG(
   dag_id="ipl_extract_and_transform",
   start_date=airflow.utils.dates.days_ago(1),
   schedule_interval=None,
)


def _extract_data():
   """Extracts the batter data and saves it to a csv.
   """
   data = extract()
   data.to_csv("./data/extracted_data.csv", index=False)


def _transform_data():
   """Loads, cleans, transforms and saves the batter data.
   """
   data = pd.read_csv("./data/extracted_data.csv")
   transformed_data = transform(data)
   transformed_data.to_csv("./data/transformed_data.csv")


extract_data = PythonOperator(
   task_id="extract_data",
   python_callable=_extract_data,
   dag=dag,
)


transform_data = PythonOperator(
   task_id="transform_data",
   python_callable=_transform_data,
   dag=dag,
)


notify = BashOperator(
   task_id="notify",
   bash_command='echo "The Indian Premier League data has been extracted and transformed."',
   dag=dag,
)

extract_data >> transform_data >> notify
