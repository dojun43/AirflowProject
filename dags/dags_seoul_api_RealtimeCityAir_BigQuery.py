from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from sensors.seoul_api_hour_sensor import SeoulApiHourSensor
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
    dag_id='dags_seoul_api_RealtimeCityAir_BigQuery',
    schedule='0 * * * *',
    start_date=pendulum.datetime(2024,3,21, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 권역별 실시간 대기환경 현황'''
    RealtimeCityAir_status_sensor = SeoulApiHourSensor(
        task_id='RealtimeCityAir_status_sensor',
        dataset_nm='RealtimeCityAir',
        base_dt_col='MSRDT',
        hour_off=0,
        poke_interval=600,
        timeout = 600*6,
        mode='reschedule'
    )

    RealtimeCityAir_status_to_csv = SeoulApiToCsvOperator(
        task_id='RealtimeCityAir_status_to_csv',
        dataset_nm='RealtimeCityAir',
        path='/opt/airflow/files/RealtimeCityAir/',
        file_name='RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ds}}.csv'
    )

    csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='csv_to_gcs',
        gcp_conn_id='googlecloud_conn_id',
        src='/opt/airflow/files/RealtimeCityAir/RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ds}}.csv',
        dst='RealtimeCityAir/RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ds}}.csv',
        bucket='dodo_bucket_1'
    )

    RealtimeCityAir_status_sensor >> RealtimeCityAir_status_to_csv >> csv_to_gcs