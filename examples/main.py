from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Europa/Kiev")
default_args = {
    'owner': 'andrii',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 25, tzinfo=local_tz),
    'email': ['shevlyakov.tlkpi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='temp_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

flight_search_ingestion= SparkSubmitOperator(task_id='search_ingestion',
conn_id='spark_local',
application='/temp.py',
total_executor_cores=4,
packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
executor_cores=2,
executor_memory='5g',
driver_memory='5g',
name='search_ingestion',
execution_timeout=timedelta(minutes=10),
dag=dag
                                             )