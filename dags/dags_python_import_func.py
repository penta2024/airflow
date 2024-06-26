from airflow.models.dag import DAG
import datetime
import pendulum

from airflow.operators.python import PythonOperator
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
       
   task_grt_sftp = PythonOperator (
       task_id ='task_grt_sftp' ,
       python_callable=get_sftp
   )
   