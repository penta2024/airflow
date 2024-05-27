from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
  @task(task_id =  'python_push' )
  def python_push_xcom() :
        result_dict = { 'status' : 'Good' , 'data' : [1,2,3] , 'option_cnt' : 100}
        return result_dict
      
  bash_pull = BashOperator(
        task_id= 'bash_pull' ,
        env = { 'STATUS' : '{{ ti.xcom_pull( task_ids = "python_push" )["status"]}}' ,
                'DATA'   : '{{ ti.xcom_pull( task_ids = "python_push" )["data"]}}' ,
                'OPTION_CNT' : '{{ ti.xcom_pull( task_ids = "python_push" )["option_cnt"]}}'
               } ,
        bash_command= 'echo $STATUS && echo $DATA && echo &OPTION_CNT '
    )       

python_push_xcom() >> bash_pull  