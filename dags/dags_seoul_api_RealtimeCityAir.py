from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from sensors.seoul_api_hour_sensor import SeoulApiHourSensor
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
    dag_id='dags_seoul_api_RealtimeCityAir',
    schedule='0 * * * *',
    start_date=pendulum.datetime(2023,12,26, tz='Asia/Seoul'),
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

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)
    
    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                'tbl_nm': 'RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ds}}',
                'file_nm':'/opt/airflow/files/RealtimeCityAir/RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ds}}.csv'
                }
    )

    RealtimeCityAir_status_sensor >> RealtimeCityAir_status_to_csv >> insrt_postgres