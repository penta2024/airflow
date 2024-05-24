from airflow.models.dag import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eq2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag : 
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs) :
       return 'Success'
       
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs) :
       ti = kwargs['ti']
       value1 = ti.xcom_pull_1(key='python_xcom_push_by_return' )
       print( 'xcom_pull 매서드로 직접 찾은 리텀값 :' + value1) 
       
    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs) :
       ti = kwargs['ti']
       print('함수 입력값으로 받은 값 :' + status)
       
    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()