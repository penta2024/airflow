from airflow.models.dag import DAG
import datetime
import pendulum
from common.common_func import regist2
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_kwarg_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag : 

 regist_t2 = PythonOperator(
      task_id ='regist_t2' ,
      python_callable=regist2 ,
      op_args=['penta' , 'man' , 'kr' , 'seoul'] ,
      op_kwargs={'email':'penta123@penta.co.kr', 'phone' : '010'}
  ) 
  
regist_t2