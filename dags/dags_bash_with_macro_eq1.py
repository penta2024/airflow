from airflow.models.dag import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eq1",
    schedule="0 0 L * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
      # START_DATE : 전월말일, END_DATE : 1일전
      bash_t1 = BashOperator(
        task_id="bash_t1",
        env = { 'START_DATE' : '{{  data_interval_start.in_timezone("Asia/Seoul") | ds}}' ,
                'END_DAT   ' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'          
        } ,
        bash_command= 'echo "START DATE : $START_DATE" && echo "END_DATE : $END_DATE" '
    )  
          


       