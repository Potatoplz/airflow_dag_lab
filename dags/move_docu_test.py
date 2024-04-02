import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from mongo_config import get_mongo_client

# Logger 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def move_document():
    client = get_mongo_client()

    db1 = client['testDB1']
    db2 = client['testDB2']

    user_document = db1.users.find_one()
    if user_document:
        db2.new_users.insert_one(user_document)
        # 로그 메시지 출력
        logger.info(f"Document with _id: {user_document['_id']} moved successfully.")
    else:
        logger.info("No more documents to move.")

        # search age

with DAG(
    'move_mongodb_document_with_logging',
    default_args=default_args,
    description='Move a document from testDB1 to testDB2 in MongoDB and log success',
    schedule_interval=timedelta(seconds=10), 
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'mongodb'],
) as dag:

    move_document_task = PythonOperator(
        task_id='move_document_with_logging',
        python_callable=move_document,
    )

move_document_task
