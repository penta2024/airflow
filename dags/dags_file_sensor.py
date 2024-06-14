import datetime
import pendulum

from airflow.models.dag           import DAG
from airflow.sensors.filesystem   import FileSensor

with DAG(
    dag_id= 'dags_file_sensor',
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
      tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id= 'tvCorona19VaccinestatNew_sensor',
        fs_conn_id = 'conn_file_opt_airflow_files' ,
        filepath = 'tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv' , 
        recursive = False,
        poke_interval = 30,     #30 sec
        timeout = 60*60*24 ,    #1  day
        mode = 'reschedule' 
      )  


      tvCorona19VaccinestatNew_sensor
 
       