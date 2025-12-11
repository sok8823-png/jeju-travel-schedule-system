# db.py
import os
import pymysql
from dotenv import load_dotenv

# .env 파일 읽기
load_dotenv()

def get_connection():
    """
    MySQL 연결 생성 함수.
    .env 에서 설정값을 읽고, 없으면 기본값 사용.
    """
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3307"))  # 호스트 포트
    user = os.getenv("DB_USER", "traveluser")
    password = os.getenv("DB_PASSWORD", "Abcd1234!")
    db_name = os.getenv("DB_NAME", "travel_ai_db")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn
