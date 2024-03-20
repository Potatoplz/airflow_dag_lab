import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

# DAG에서 실행할 Python 함수 정의
def print_potato():
    print("========================= Potato please! =========================")

# DAG 설정 (기본 args 설정)
default_args = {
    'owner': 'CYW',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 19),
}

# DAG 인스턴스 생성
with DAG(
    dag_id='interval_log_dag',
    default_args=default_args,
    description='A simple DAG that prints something every 3 seconds',
    schedule_interval=timedelta(seconds=3),  # 3초마다 실행
    catchup=False,
    tags=['test']
) as dag:

    # PythonOperator를 사용하여 print_potato 함수를 실행하는 태스크 정의
    print_potato_task = PythonOperator(
        task_id='print_potato',
        python_callable=print_potato,
    )