from pymongo import MongoClient

# MongoDB 연결 설정
mongo_user = 'admin'
mongo_pwd = 'qwer1234'
mongo_host = 'localhost'
mongo_port = '27017'
auth_db = 'admin'

# MongoClient 인스턴스 생성 함수
def get_mongo_client():
    return MongoClient(
        host=mongo_host,
        port=int(mongo_port),
        username=mongo_user,
        password=mongo_pwd,
        authSource=auth_db
    )
