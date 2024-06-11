from airflow.models.dag import DAG
import datetime
import pendulum

from airflow.operators.python           import PythonOperator
from hooks.custom_postgres_hook         import CustomPostgresHook

with DAG(
    dag_id="dags_python_with_custom_hook_bulk_load",
    schedule="0 7 * * *",
 #   schedule= None ,    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
   def insert_postgres( postgres_conn_id, tbl_nm, file_nm, **kwargs) :
       custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
       custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter= ',', is_header=True, is_replace=True)
            
   insert_postgres_with_hook = PythonOperator (
       task_id ='insert_postgres_with_hook' ,
       python_callable=insert_postgres ,
       op_kwargs = { 'postgres_conn_id' : 'conn-db-postgres-custom' ,
                     'tbl_nm' : 'TbCorona19CountStatus_bulk2' , 
                     'file_nm': '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'
                    }
   )
   
   insert_postgres_with_hook
   