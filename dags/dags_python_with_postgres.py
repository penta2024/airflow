from airflow.models.dag import DAG
import datetime
import pendulum

from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_postgres",
 #   schedule="30 6 * * *",
    schedule= None ,    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
   def insert_postgres(ip, port, dbname, user, passwd, **kwargs) :
       import psycopg2 
       from contextlib import closing
       
       with closing( psycopg2.connect (host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn :
           with closing(conn.cursor()) as cursor :
               dag_id  = kwargs.get('ti').dag_id
               task_id = kwargs.get('ti').task_id
               run_id  = kwargs.get('ti').run_id
               msg = 'insert 수행'
               sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
               cursor.execute(sql, (dag_id, task_id, run_id, msg))
               conn.commit()
               
       
   insert_postgres = PythonOperator (
       task_id ='insert_postgres' ,
       python_callable=insert_postgres ,
       op_orgs = ['172.28.0.3' ,'5433' , 'penta123', 'penta123', 'penta123']
   )
   
   insert_postgres
   