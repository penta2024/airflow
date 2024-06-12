import datetime
import pendulum

from airflow.models.dag     import DAG
from airflow.sensors.bash   import BashSensor
from airflow.operators.bash import BashOperator

with DAG(
    dag_id= 'dags_bash_sensor',
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
      sensor_task_by_poke = BashSensor(
        task_id= 'sensor_task_by_poke',
        env = {'FILE' : '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'} , 
        bash_command= f'''echo $FILE &&
                          if [ -f $FILE ] ; then
                              exit 0
                          else 
                              exit 1
                          fi''' ,
        poke_interval = 30, #30 sec
        timeout = 60*2 ,    #2  min
        mode = 'poke' ,
        soft_fail = False
      )  

      sensor_task_by_reschedule = BashSensor(
        task_id= 'sensor_task_by_reschedule',
        env = {'FILE' : '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'} , 
        bash_command= f'''echo $FILE &&
                          if [ -f $FILE ] ; then
                              exit 0
                          else 
                              exit 1
                          fi''' ,
        poke_interval = 30, #30 sec
        timeout = 60*9 ,    #9  min
        mode = 'reschedule' ,
        soft_fail = True
      )  
      
      bash_task = BashOperator(
        task_id='bash_task',
        env = {'FILE' : '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'} , 
#        bash_command= 'echo "건수: 'cat $FILE | wc -l '"' ,
        bash_command= 'echo cat $FILE | wc -l' ,   
      )   
      
      [sensor_task_by_poke, sensor_task_by_reschedule] >> bash_task
 
       