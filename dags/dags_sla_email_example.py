from airflow.models.dag      import DAG
#from airflow.operators.email import EmailOperator
from airflow.operators.bash  import BashOperator
#from airflow.decorators      import task
#from airflow.exceptions      import AirflowException
from datetime                import timedelta
from airflow.models          import Variable

import datetime
import pendulum

email_str = Variable.get('email_target')
email_lst = [email.strip() for email in email_str.split(',') ]

with DAG(
    dag_id="dags_sla_email_example",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=2) ,
    default_args = { 'sla'   : timedelta(seconds=70) ,
                     'email' : email_lst
        } ,
    catchup=False
) as dag :
    
    bash_slp_30s_sla_70s = BashOperator(
        task_id= 'bash_slp_30s_sla_70s',
        bash_command = 'sleep 30'
    )  
    
    bash_slp_60s_sla_70s = BashOperator(
        task_id= 'bash_slp_60s_sla_70s',
        bash_command = 'sleep 60'
    )     
    
    bash_slp_10s_sla_70s = BashOperator(
        task_id= 'bash_slp_10s_sla_70s',
        bash_command = 'sleep 10'
    )     
      
    bash_slp_10s_sla_30s = BashOperator(
        task_id= 'bash_slp_10s_sla_30s',
        bash_command = 'sleep 10' ,
        sla = timedelta(seconds =30)
    )         
    
    bash_slp_30s_sla_70s  >> bash_slp_60s_sla_70s  >> bash_slp_10s_sla_70s  >> bash_slp_10s_sla_30s