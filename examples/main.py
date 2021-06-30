from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Asia/Tehran")
default_args = {
    'owner': 'mahdyne',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='flight_search_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

pyspark_app_home=Variable.get("PYSPARK_APP_HOME")

flight_search_ingestion= SparkSubmitOperator(task_id='flight_search_ingestion',
conn_id='spark',
application=f'{pyspark_app_home}temp.py',
total_executor_cores=4,
packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
executor_cores=2,
executor_memory='5g',
driver_memory='5g',
name='flight_search_ingestion',
execution_timeout=timedelta(minutes=10),
dag=dag
)