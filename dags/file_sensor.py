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

# Set clickhouse host (default docker bridge ip)
clickhouse_conn = "172.18.0.2:8123"

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

def test_length(**context):
    init_length = context['task_instance'].xcom_pull(key='return_value', task_ids='check_length')
    if init_length == "0":
        raise AE(f"Cannot read empty file: {filepattern} on path: {filepath}")

test_is_empty = PythonOperator(
    task_id='is_file_empty', python_callable=test_length, dag=dag)

create_raw_tbl_ch = BashOperator(task_id='create_raw_table_CH',
    bash_command=f"cat dags/config/clickhouse_scripts/createRaw.tbl | \
        curl '{clickhouse_conn}' --data-binary @-",
    dag=dag)

insert_raw_data_ch = BashOperator(task_id='insert_raw_data_CH',
    bash_command=f"cat data/event-data.json | \
        curl '{clickhouse_conn}?query=INSERT+INTO+userEvents+FORMAT+JSONEachRow' --data-binary @-",
    dag=dag)

delete_raw_tbl_ch = BashOperator(task_id='delete_raw_table_CH',
    bash_command=f"curl '{clickhouse_conn}?query=DROP+TABLE+userEvents'",
    dag=dag)

trigger = TriggerDagRunOperator(
    task_id='trigger_dag_rerun', trigger_dag_id=task_name, dag=dag)


sensor_file >> check_length >> test_is_empty \
        >> create_raw_tbl_ch >> insert_raw_data_ch \
        >> delete_raw_tbl_ch >> trigger
