from config.kakao_api  import send_kakao_msg

def on_failure_callback_to_kakao(context) :
    exception = context.get('exception') or 'exception 없음'
    ti = context.get('ti')
    dag_id  = ti.dag_id
    task_id = ti.task_id
 #   err_msg = context.get('execption')
    data_interval_end = context.get('data_interval_end').in_timezone('Asia/Seoul')
    
    content = {f'{dag_id}.{task_id}' : f'에러내용 :{exception}', '':''  }                #content 길이는 2이상잉야 함
    send_kakao_msg(talk_title=f'task 실패 알람({data_interval_end})' , content=content)