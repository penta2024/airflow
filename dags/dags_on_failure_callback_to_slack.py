from airflow.models.dag                  import DAG
from airflow.operators.bash              import BashOperator
from datetime                            import timedelta
from config.on_failure_callback_to_slact import on_failure_callback_to_slact
import pendulum


with DAG(
    dag_id="dags_on_failure_callback_to_slack",
    schedule="*/20 * * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args = {
      'on_failure_callback' : on_failure_callback_to_slact ,
      'execution_timeout'   : timedelta(seconds=60)
    }
) as dag :
    
      bash_slp_90 = BashOperator(
        task_id='bash_slp_90',
        bash_command='sleep 90',
    )  
      
      bash_ext_1 = BashOperator(
        trigger_rule = 'all_done' ,
        task_id='bash_ext_1',
        bash_command='exit 1'
    )       

bash_slp_90 >> bash_ext_1 

       