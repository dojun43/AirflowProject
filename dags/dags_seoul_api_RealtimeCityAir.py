from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_RealtimeCityAir',
    schedule='1 * * * *',
    start_date=pendulum.datetime(2023,12,26, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 권역별 실시간 대기환경 현황'''
    RealtimeCityAir_status_to_csv = SeoulApiToCsvOperator(
        task_id='RealtimeCityAir_status_to_csv',
        dataset_nm='RealtimeCityAir',
        path='/opt/airflow/files/RealtimeCityAir/',
        file_name='RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ts_nodash}}.csv'
    )