from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

'''
서울시 공공데이터 API 추출시 특정 시간 컬럼을 조사하여 데이터가 존재하는지 체크하는 센서 
'''

class SeoulApiHourSensor(BaseSensorOperator):
    template_fields = ('endpoint',)
    def __init__(self, dataset_nm, base_dt_col, hour_off=0, **kwargs):
        '''
        dataset_nm: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col: 센싱 기준 컬럼 (yyyy.mm.dd... or yyyy/mm/dd... 형태만 가능)
        day_off: 배치일 기준 생성여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        '''
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm + '/1/100'   # 100건만 추출
        self.base_dt_col = base_dt_col
        self.hour_off = hour_off

        
    def poke(self, context):
        import requests
        import json
        from dateutil.relativedelta import relativedelta

        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
        self.log.info(f'request url:{url}')
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_time = row_data[0].get(self.base_dt_col)
        search_ymdh = (context.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta(hours=self.hour_off)).strftime('%Y-%m-%d %H:00')
        
        try:
            import pendulum
            pendulum.from_format(last_time, 'YYYYMMDDHHmm')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{self.base_dt_col} 컬럼은 YYYYMMDDHHmm 형태가 아닙니다.')

        if last_time >= search_ymdh:
            self.log.info(f'생성 확인(기준 날짜: {search_ymdh} / API Last 날짜: {last_time})')
            return True
        else:
            self.log.info(f'Update 미완료 (기준 날짜: {search_ymdh} / API Last 날짜:{last_time})')
            return False