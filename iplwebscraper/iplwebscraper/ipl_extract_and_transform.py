"""DAG definition for extract and transform pipeline.
"""
# pylint: disable=pointless-statement
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from extract import extract
from transform import transform


dag = DAG(
   dag_id="ipl_extract_and_transform",
   start_date=airflow.utils.dates.days_ago(1),
   schedule_interval=None,
)


extract_data = PythonOperator(
   task_id="extract_data",
   python_callable=extract,
   dag=dag,
)


transform_data = PythonOperator(
   task_id="transform_data",
   python_callable=transform,
   dag=dag,
)


notify = BashOperator(
   task_id="notify",
   bash_command='echo "The Indian Premier League data has been extracted and transformed."',
   dag=dag,
)

extract_data >> transform_data >> notify
