from airflow.models.dag import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eq2",
    schedule="0 0 L * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
      # START_DATE : 2주전 월요일, END_DATE : 2주전 토요일
      bash_t2 = BashOperator(
        task_id="bash_t2",
        env = { 'START_DATE' : '{{ (data_inyerval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}'  ,
                'END_DAT   ' : '{{ (data_inyerval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds}}'          
        } ,
        bash_command= 'echo "START DATE : $START_DATE" && echo "END_DATE : $END_DATE" '
    )  
          


       