from airflow                   import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

import pendulum

with DAG (
    dag_id     = 'dags_timeout_sensor_with_async',
    start_date = pendulum.datetime(2024, 6, 1, 0, 0, 0) ,
    end_date   = pendulum.datetime(2024, 6, 1, 1, 0, 0) ,
    schedule   = '*/10 * * * *' ,
    catchup = True ,
) as dag :
    
    sync_sensor   = DateTimeSensorAsync (
        task_id= 'sync_sensor',
        target_time  = """ {{macros.datetime.utcnow() + macros.timedelta(minutes=5)}}    """  
    )  
    
