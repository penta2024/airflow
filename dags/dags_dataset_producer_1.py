from airflow.models.dag     import DAG
from airflow                import Dataset
from airflow.operators.bash import BashOperator
import datetime
import pendulum

dataset_dags_dataset_producer_1 = Dataset('dags_dataset_producer_1')

with DAG(
    dag_id= 'dags_dataset_producer_1' ,
    schedule= '0 7 * * *' ,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag :
    
      bash_task = BashOperator(
        task_id= 'bash_task' ,
        outlets = [dataset_dags_dataset_producer_1] ,
        bash_command = 'echo "produs=cer_1 수행 완료"'
    )  

       