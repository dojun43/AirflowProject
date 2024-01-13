from sensors.seoul_api_hour_sensor import SeoulApiHourSensor
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    RealtimeCityAir_status_sensor = SeoulApiHourSensor(
        task_id='RealtimeCityAir_status_sensor',
        dataset_nm='RealtimeCityAir',
        base_dt_col='MSRDT',
        hour_off=0,
        poke_interval=600,
        mode='reschedule'
    )

    RealtimeCityAir_status_to_csv = SeoulApiToCsvOperator(
        task_id='RealtimeCityAir_status_to_csv',
        dataset_nm='RealtimeCityAir',
        path='/opt/airflow/files/RealtimeCityAir/',
        file_name='RealtimeCityAir_{{data_interval_end.in_timezone("Asia/Seoul") | ds}}.csv'
    )

    RealtimeCityAir_status_sensor >> RealtimeCityAir_status_to_csv
    