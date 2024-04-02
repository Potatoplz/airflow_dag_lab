from datetime import datetime, timedelta
from bson import ObjectId
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from mongo_config import get_mongo_client

# MongoDB에 데이터를 삽입하는 함수
def insert_data_mongodb(**kwargs):
    print("Script started")
    client = get_mongo_client()
    db = client.departureDB
    collection = db.departureTB

    success_count = 0
    fail_count = 0
    
    for _ in range(100):  # 100건의 데이터를 삽입하는 루프
        data = {
            "_id": ObjectId(),
            "name": f"Name_{random.randint(1, 100)}",
            "age": random.randint(20, 60),
            "insert_date": datetime.now()
        }
        try:
            collection.insert_one(data)
            success_count += 1
        except Exception as e:
            print(f"Failed to insert data: {e}")
            fail_count += 1
    
    # 성공 및 실패 건수를 콘솔에 출력
    print(f"Data insert completed. Success: {success_count}, Fail: {fail_count}")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mongodb_insert_dag',
    default_args=default_args,
    description='A DAG to insert data into MongoDB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 3, 25),
    catchup=False,
)

# 데이터 삽입 태스크
insert_data_task = PythonOperator(
    task_id='insert_data_mongodb',
    python_callable=insert_data_mongodb,
    dag=dag,
)

insert_data_task
