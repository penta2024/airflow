# Package Import
from airflow.models.dag import DAG
import datetime
import pendulum


from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_seoul_api_corona_copy",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
         
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )  
      
    bash_t1  