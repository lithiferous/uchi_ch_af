from airflow import DAG
from airflow import AirflowException as AE
from airflow.operators import PythonOperator
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

def get_filename(config_file):
    return Variable.get(config_file, deserialize_json=True)

# Set the event file variable
data_conf = get_filename("file_config")
filename = data_conf["sourcePath"] + data_conf["filePattern"]

# Set the spark script variable
script_conf = get_filename("spark_script")
process_script = script_conf["sourcePath"] + script_conf["filePattern"]

# Set clickhouse host (default docker bridge ip)
clickhouse_conn = "172.18.0.2:8123"

dag = DAG(
    'read_events',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1)

sensor_event_file = OmegaFileSensor(
    task_id="file_sensor",
    filepath=data_conf["sourcePath"],
    filepattern=data_conf["filePattern"],
    poke_interval=5,
    dag=dag)

check_length = BashOperator(task_id='check_length',
    bash_command="wc -l < '%s'" %filename,
    xcom_push=True,
    dag=dag)

def test_length(**context):
    init_length = context['task_instance'].xcom_pull(key='return_value', task_ids='check_length')
    if init_length == "0":
        raise AE(f"Cannot read empty file: {data_conf['filePattern']} on path: {data_conf['sourcePath']}")

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

process_data = BashOperator(task_id='submit_spark_job',
    bash_command=f"spark-shell -i {process_script}",
    dag=dag)

output_file_dir = data_conf["sourcePath"] + data_conf["filePattern"].replace("json", "pq"),
test_process_complete = OmegaFileSensor(
    task_id="output_file_sensor",
    filepath=output_file_dir,
    filepattern='_SUCCESS',
    poke_interval=5,
    dag=dag)

create_clean_tbl_ch = BashOperator(task_id='create_clean_table_CH',
    bash_command=f"cat dags/config/clickhouse_scripts/create.tbl | \
        curl '{clickhouse_conn}' --data-binary @-",
    dag=dag)

insert_clean_data_ch = BashOperator(task_id='insert_clean_data_CH',
    bash_command=f"cd {output_file_dir} && ls | grep '[a-z]' | xargs head -1 | \
        curl '{clickhouse_conn}?query=INSERT+INTO+userEvents+FORMAT+Parquet' --data-binary @-",
    dag=dag)

delete_raw_tbl_ch = BashOperator(task_id='delete_raw_table_CH',
    bash_command=f"curl '{clickhouse_conn}?query=DROP+TABLE+userEvents'",
    dag=dag)

sensor_event_file >> check_length >> test_is_empty \
                  >> create_raw_tbl_ch >> insert_raw_data_ch \
                  >> process_data >> test_process_complete \
                  >> [create_clean_tbl_ch, delete_raw_tbl_ch] \
                  >> insert_clean_data_ch 
