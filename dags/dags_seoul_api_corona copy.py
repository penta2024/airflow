# Package Import
from airflow.models.dag import DAG
import datetime
import pendulum

from operators.seoul_api_to_csv_operator  import SeoulApiToCsvOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_seoul_api_corona_copy",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''  서울시 코로나19 확진자 발생 정보  '''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id = 'tb_corona19_count_status'  ,
        dataset_nm = 'TbCorona19CountStatus'  ,
        file_name = 'TbCorona19CountStatus.csv'  ,
        path = '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}' 
    )  
     
    '''  서울시 코로나19 예방접종 협황정보 '''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id = 'tv_corona19_vaccine_stat_new'  ,
        dataset_nm = 'tvCorona19VaccinestatNew'  ,
        file_name = 'tvCorona19VaccinestatNew.csv'  ,
        path = '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}' 
    )  
         
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )  
      
    bash_t1  >> tb_corona19_count_status  >> tv_corona19_vaccine_stat_new