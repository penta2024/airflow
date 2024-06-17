import datetime
import pendulum

from airflow.models.dag              import DAG
from sensors.seoul_api_date_sensor   import SeoulApiDateSensor


with DAG(
    dag_id= 'dags_custom_sensor',
#    schedule="10 1 * * *",
    schedule= None ,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag :
  
      TbCorona19CountStatus_count_sensor = SeoulApiDateSensor (
        task_id= 'TbCorona19CountStatus_count_sensor',
        dataset_nm = 'TbCorona19CountStatus' ,
        base_dt_col  = 'S_DT' ,
        day_off = 0 ,
        poke_interval = 600,     #10 min 
        mode = 'reschedule'         
      )   
            
      tvCorona19VaccinestatNew_count_sensor = SeoulApiDateSensor (
        task_id= 'tvCorona19VaccinestatNew_count_sensor',
        dataset_nm = 'tvCorona19VaccinestatNew' ,
        base_dt_col  = 'S_VC_DT' ,
        day_off = -1 ,
        poke_interval = 600,     #10 min 
        mode = 'reschedule'         
      )   
 
       