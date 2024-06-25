from airflow.providers.slack.hooks.slack_webhook  import SlackWebhookHook

def on_failure_callback_to_slact(context) :
    ti = context.get('ti')
    dag_id  = ti.dag_id
    task_id = ti.task_id
    err_msg = context.get('execption')
    batch_date = context.get('data_interval_end').in_timezone('Asia/Seoul')
    
    slack_hook = SlackWebhookHook(slack_webhook_conn_id = 'conn_slack_airflow_bot')
    text = "실패 알람"
    blocks = [
        {
            "type" : "section" ,
            "text" : {
                "type" : "mrkdwn" ,
                "text" : f"*{dag_id}.{task_id} 실패 알람"
            }
        } ,
        {
            "type" : "section" ,
            "field" : [
                {
                    "type" : "mrkdwn" ,
                    "text" : f"*배치 시간*: {batch_date}"
                } ,
                {
                    "type" : "mrkdwn" ,
                    "text" : f"*에러 배용*: {err_msg}"
                }
            ]
        }
    ]
    
    slack_hook.send(text=text, blocks=blocks)