from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_templete",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag : 
    
    def python_function1(start_date, end_date, **kwargs) :
        print(start_date)
        print(end_date)
        
    python_t1 = PythonOperator (
        task_id ='python_t1' ,
        python_callable =  python_function1 ,
        op_kwargs = {'start_date' : '{{data_interval_start | ds}}'  , 'end_date' : '{{data_interval_end | ds}}' }  
    )

    python_t2 = PythonOperator (
        task_id ='python_t2' ,
        python_callable =  python_function1 ,
        op_kwargs = {'start_date' : '{{data_interval_start | ds}}'  , 'end_date' : '{{data_interval_end | ds}}' }  
    )
   
          
    python_t1 >> python_t2