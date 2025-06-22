from dotenv import load_dotenv
import os
import pymysql


class DatabaseManager:
    """MySQL RDS 연결 및 데이터베이스 관련 기능"""

    def __init__(self, host=None, port=None, username=None, password=None, database=None):
        """
        DatabaseManager 클래스 초기화

        Args:
            host (str): 데이터베이스 서버 주소 (환경 변수: `host`)
            port (int): 데이터베이스 포트 (환경 변수: `port`)
            username (str): 데이터베이스 사용자명 (환경 변수: `username`)
            password (str): 데이터베이스 비밀번호 (환경 변수: `password`)
            database (str): 사용할 데이터베이스명 (환경 변수: `database`)
        """
        load_dotenv()  # .env 파일 로드 (있을 경우)

        self.host = host or os.environ.get("host")
        self.port = int(port or os.environ.get("port", 3306))  # 기본값 3306
        self.username = username or os.environ.get("username")
        self.password = password or os.environ.get("password")
        self.database = database or os.environ.get("database")

        self.connection = None
        self.connect()  # 클래스 생성 시 자동 연결

    def connect(self):
        """MySQL RDS 데이터베이스 연결"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            print(f"✅ [INFO] Database connected successfully: {self.host}:{self.port} (DB: {self.database})")
        except pymysql.MySQLError as e:
            print(f"❌ [ERROR] Database connection failed: {e}")
            self.connection = None

    def execute_query(self, query, params=None, fetch_one=False):
        """
        데이터베이스에서 SQL 쿼리를 실행하고 결과를 반환.

        Args:
            query (str): 실행할 SQL 쿼리문
            params (tuple, optional): SQL 쿼리의 파라미터
            fetch_one (bool): True이면 첫 번째 결과만 반환, False이면 전체 결과 반환

        Returns:
            list or dict: 쿼리 결과 데이터 (SELECT 문일 경우)
        """
        if not self.connection:
            print("❌ [ERROR] Database connection is not available.")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchone() if fetch_one else cursor.fetchall()
        except pymysql.MySQLError as e:
            print(f"❌ [ERROR] Query execution failed: {e}")
            return None

    def get_latest_propose_date(self):
        """RDS 데이터베이스에서 가장 최근의 법안 발의 날짜를 가져오는 함수"""
        try:
            query = "SELECT MAX(propose_date) AS latest_date FROM Bill"
            result = self.execute_query(query, fetch_one=True)
            return result["latest_date"] if result else None
        except Exception as e:
            print("❌ [ERROR] Failed to fetch the latest propose_date")
            print(e)
            return None

    def get_latest_timeline_date(self):
        """RDS 데이터베이스에서 가장 최근의 법안 처리 날짜를 가져오는 함수"""
        try:
            query = "SELECT MAX(status_update_date) AS latest_date FROM BillTimeline"
            result = self.execute_query(query, fetch_one=True)
            return result["latest_date"] if result else None
        except Exception as e:
            print("❌ [ERROR] Failed to fetch the latest status_update_date")
            print(e)
            return None
    
    def get_existing_bill_ids(self, bill_ids):
            """데이터베이스에 이미 존재하는 법안 id를 반환하는 함수"""

            format_strings = ','.join(['%s'] * len(bill_ids)) 

            # **수정된 부분:** query를 문자열로만 만들고, bill_ids 튜플을 params로 전달
            query = f"SELECT {'bill_id'} FROM Bill WHERE {'bill_id'} IN ({format_strings})"
            params = tuple(bill_ids) # 매개변수를 별도의 변수로 분리

            result = self.execute_query(query, params=params, fetch_one=False) # params 인자를 명시적으로 전달
            # print(result)

            # Extract IDs from the result
            existing_ids = [row['bill_id'] for row in result]

            print(f"DB에 존재하는 법안 id 목록: {existing_ids}")

            return existing_ids

    def close(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            print("✅ [INFO] Database connection closed.")