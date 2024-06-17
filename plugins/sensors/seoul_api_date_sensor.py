from airflow.sensors.base           import BaseSensorOperator
from airflow.hooks.base             import BaseHook
import pandas as pd 

'''
서울시 공공데이터 api 추출시 특정날짜 컬럼을 조사하여
배치 날짜 기준 전날 데이터가 존재하는지 체크하는 센서
1. 데이터셋에 날짜 컬럼이 존재하고
2. API 사용시 그 날짜 컬럼ㅇ로 order by dec 되어 가져온다는 가정하에 사용 가능
'''
class SeoulApiDateSensor (BaseSensorOperator):
    template_fields = ('endpoint')
        
    def __init__(self, dataset_nm, path, base_dt_col, day_off=0,  **kwargs) :
        '''
        dateset_nm : 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col : 센싱 기준 컬럼 ( yyyy.mm.dd...or yyyy/mm/dd... 형태민 가능)
        day_off : 배치일 기준 생성여부를 확인하고자 하는 날짜 차이를 입력(기본값 : 0)
        '''
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm + '/1/100'
        self.base_dt_col = base_dt_col
        self.day_off = day_off

    def poke(self, context) :
        import requests
        import json
        from dateutil.relativedelta  import relativedelta
        
        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        self.log.info(f'requests url : {url}')
        response = requests.get(url)
        
        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_dt = row_data[0].get(base_dt_col)
        last_date = last_dt[:10]
        last_date = last_date.replace('.' , '-').replace('/' , '-')
        search_ymd = ( context.get('data_interval_end').in_timezone('Asia/Seoul') +relativedelta(days=self.day_off)).strftime('%Y-%m-%d')
        try :
          import pendulum
          pendulum.from_format(last_date, 'YYYY-MM-DD')
        except :
          from airflow.exceptions import AirflowException
          AirflowException(f'{self.base_dt_col} 칼럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다. ')
          
        if last_date >= search_ymd :
           self.log.nfo(f'생성 확인 (배치 날짜 : {search_ymd} / API last 날짜 : {last_date})') 
           print(f'생성 확인 (배치 날짜 : {search_ymd} / API last 날짜 : {last_date})') 
           return True
        else :
           self.log.info(f'Update 확인 (배치 날짜 : {search_ymd} / API last 날짜 : {last_date})') 
           print(f'Update 확인 (배치 날짜 : {search_ymd} / API last 날짜 : {last_date})') 
           return False
       
       
     