from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_templete_copy",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag : 
    

    @task(task_id='python_t2')
    def python_function2(**kwargs):  
        print(kwargs)
        print( 'ds:' + kwargs['ds'])
        print( 'ts:'  + kwargs['ts'])
        print( 'data_interval_start:' + str(kwargs['data_interval_start']))
        print( 'data_interval_end:' + str(kwargs['data_interval_end']))       
        print( 'task_instance:' + str(kwargs['ti']))  
          
    python_function2
    
