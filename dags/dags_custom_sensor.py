from sensors.seoul_api_hour_sensor import SeoulApiHourSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    status_sensor = SeoulApiHourSensor(
        task_id='status_sensor',
        dataset_nm='RealtimeCityAir',
        base_dt_col='MSRDT',
        day_off=0,
        poke_interval=600,
        mode='reschedule'
    )