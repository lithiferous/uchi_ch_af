import json

from airflow import DAG
from airflow import AirflowException as AE
from airflow.operators import PythonOperator, TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from filesensor_plugin import OmegaFileSensor

from datetime import datetime, timedelta

default_args = {
    'owner': 'glsam',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 16),
    'provide_context': True,
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

task_name = 'file_sensor_task'

# Set the file variable
dag_config = Variable.get("example_variables_config", deserialize_json=True)
filepath = dag_config["sourcePath"]
filepattern = dag_config["filePattern"]
filename = filepath + filepattern

dag = DAG(
    'read_events',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1)

sensor_file = OmegaFileSensor(
    task_id=task_name,
    filepath=filepath,
    filepattern=filepattern,
    poke_interval=3,
    dag=dag)

check_length = BashOperator(task_id='check_length',
    bash_command="wc -l < '%s'" %filename,
    xcom_push=True,
    dag=dag)

def test_callable(**context):
    init_length = context['task_instance'].xcom_pull(key='return_value', task_ids='check_length')
    print("________________")
    if init_length == "543705":
        raise AE(f"Cannot read empty file: {filepattern} on path: {filepath}")

test_is_empty = PythonOperator(
    task_id='is_file_empty', python_callable=test_callable, dag=dag)

trigger = TriggerDagRunOperator(
    task_id='trigger_dag_rerun', trigger_dag_id=task_name, dag=dag)

sensor_file >> check_length >> test_is_empty >> trigger
