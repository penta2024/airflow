from airflow                   import DAG
from airflow.sensors.date_time import DateTimeSensor

import pendulum

with DAG (
    dag_id     = 'dags_timeout_sensor',
    start_date = pendulum.datetime(2024, 6, 1, 0, 0, 0) ,
    end_date   = pendulum.datetime(2024, 6, 1, 1, 0, 0) ,
    schedule   = '*/10 * * * *' ,
    catchup = True ,
) as dag :
    
    sync_sensor   = DateTimeSensor(
        task_id= 'sync_sensor',
        target_time  = """ {{macros.datetime.utcnow() + macros.timedelta(minutes=5)}}    """  
    )  
    
