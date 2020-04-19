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
dag_config = Variable.get("example_variables_config", deserialize_json=True)
filepath = dag_config["sourcePath"]
filepattern = dag_config["filePattern"]

dag = DAG(
    'read_events',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1)

sensor_task = OmegaFileSensor(
    task_id=task_name,
    filepath=filepath,
    filepattern=filepattern,
    poke_interval=3,
    dag=dag)

def test_is_empty_file(**context):
    file_to_process = context['task_instance'].xcom_pull(
        key='file_name', task_ids=task_name)
    filename = filepath + file_to_process
    bash_operator = BashOperator(task_id='check_length',
        bash_command="wc -l < '%s'" %filename,
        xcom_push=True,
        dag=dag)
    bash_operator.execute(context=context)

test_is_empty = PythonOperator(
    task_id='is_empty_file', python_callable=test_is_empty_file, dag=dag)

trigger = TriggerDagRunOperator(
    task_id='trigger_dag_rerun', trigger_dag_id=task_name, dag=dag)

sensor_task >> test_is_empty >> trigger
