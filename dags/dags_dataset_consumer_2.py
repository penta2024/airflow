from airflow.models.dag     import DAG
from airflow                import Dataset
from airflow.operators.bash import BashOperator
import datetime
import pendulum

dataset_dags_dataset_producer_1 = Dataset('dags_dataset_producer_1')
dataset_dags_dataset_producer_2 = Dataset('dags_dataset_producer_2')

with DAG(
    dag_id= 'dags_dataset_consumer_2' ,
#    schedule= '0 7 * * *' ,
    schedule = [dataset_dags_dataset_producer_1, dataset_dags_dataset_producer_2] ,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag :
    
      bash_task = BashOperator(
        task_id= 'bash_task' ,
        bash_command = 'echo {{ti.run_id}} && echo "producer_1, producer_2 이 완료되면 수행"'
    )  

       