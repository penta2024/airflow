from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.decorators import task
from airflow.operators.email import EmailOperator
import random

with DAG(
    dag_id="dags_python_email_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

   
  @task(task_id =  'something_task' )
  def something_logic(**kwargs) :
      from random import choice
      return choice (['Success','Fail'])
      
  send_email = EmailOperator ( 
        task_id= 'send_email' ,
        to ='nandari777@penta.co.kr' ,
        subject = '{{data_interval_end.in_timezone("Asia/Seoul") | ds}} some_logic 처리결과' , 
        html_content = '{{data_interval_end.in_timezone("Asia/Seoul") | ds}} 처리결과는 <br> \
                        {{ ti.xcom_pull(task_ids ="something_task") }} 했습니다 <br> '
     )       

something_logic() >> send_email  