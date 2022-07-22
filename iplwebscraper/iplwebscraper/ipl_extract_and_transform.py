import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from extract import obtain_batting_results
from transform import transform


dag = DAG(
   dag_id="ipl_extract_and_transform",
   start_date=airflow.utils.dates.days_ago(1),
   schedule_interval=None,
)


def _obtain_batting_results():
    data = obtain_batting_results()


def _transform():
    transformed_data = transform(data)
    

extract_data = PythonOperator(
   task_id="extract_data",
   python_callable=_obtain_batting_results,
   dag=dag,
)


transform_data = PythonOperator(
   task_id="transform_data",
   python_callable=_transform,
   dag=dag,
)


notify = BashOperator(
   task_id="notify",
   bash_command='echo "The Indian Premier League data has been extracted and transformed."',
   dag=dag,
)
 
extract_data >> transform_data >> notify 
