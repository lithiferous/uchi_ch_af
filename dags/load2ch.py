from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from clickhouse_plugin.operators.clickhouse_load_operator import ClickHouseLoadCsvOperator

DAG_ID = 'testing_clickhouse_plugin'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
}

dag = DAG(DAG_ID, **{
    'default_args': default_args,
    'schedule_interval': '@daily',
    'catchup': False,
})


with dag:
    ClickHouseLoadCsvOperator(**{
        'task_id': 'load_csv',
        'clickhouse_conn_id': 'clickhouse_localhost',
        'filepath': '/path/to/some/sample.csv',
        'database': 'default',
        'table': 'test',
        'schema': {
            'x': lambda x: int(float(x))
        },
    })
