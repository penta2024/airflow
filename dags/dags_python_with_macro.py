from airflow.models.dag import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag : 

    @task(task_id='task_using_macro' ,
        teplete_dict = { 'start_date' : '{{  data_interval_start.in_timezone("Asia/Seoul") | ds}}' ,
                         'end_date'   : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'   
        }
    )
    def get_datetime_macro(**kwargs):  
        teplete_dict = kwargs.get('teplete_dict') or {}
        if teplete_dict :
            start_date = teplete_dict.get('start_date') or 'start_date 없음'
            end_date   = teplete_dict.get('end_date')   or 'end_date 없음'
            print(start_date)
            print(end_date)
            
    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):  
        from dateutil.relativedelta import relativedelta
        
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul")                + relativedelta(months= -1, day=1)
        prev_month_day_last  = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta( day=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d')) 
        +  print(prev_month_day_last.strftime('%Y-%m-%d'))
    
    get_datetime_macro() >> get_datetime_calc()
    