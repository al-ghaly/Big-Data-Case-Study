from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import sys
sys.path.append('/opt/airflow/includes')
from Data_Load import automate

default_args = {
    'owner': 'al-ghaly',
    'start_date': datetime(2024, 6, 25),
    'email_on_failure': True,
    'email': 'alghaly_m@yahoo.com',
}

dag = DAG(
    'AirflowScript',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,  # Prevent backfilling
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

pull_files_task = BashOperator(
    task_id='pull_files',
    bash_command="./scripts/pull_files.BASH",
    dag=dag,
)

waiting_for_aws_data = FileSensor(
    task_id='waiting_for_files',
    fs_conn_id='fs_default',
    filepath='/data/Spark_Project/*',
    poke_interval=10,
    timeout=30,
    dag=dag,
)

add_to_data_lake = PythonOperator(
    task_id='upload_to_HDFS',
    python_callable=automate,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Setting up dependencies
start >> pull_files_task >> waiting_for_aws_data >> add_to_data_lake >> end
