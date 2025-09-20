import os
from dotenv import load_dotenv

# .env 파일이 존재하면 해당 파일에서 환경 변수를 로드합니다.
# 이 함수는 스크립트가 시작될 때 호출되어야 합니다.
load_dotenv()

# --- OpenAI API 설정 ---
# OpenAI API 키를 환경 변수에서 가져옵니다.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# 사용할 임베딩 모델의 이름을 환경 변수에서 가져옵니다. 기본값은 "text-embedding-3-small"입니다.
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")


# --- Qdrant 벡터 데이터베이스 설정 ---
# Qdrant 서비스의 호스트 주소를 환경 변수에서 가져옵니다.
QDRANT_HOST = os.getenv("QDRANT_HOST")
# Qdrant API 키를 환경 변수에서 가져옵니다. (선택 사항)
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
# .env에서 QDRANT_USE_HTTPS 값을 읽어와 True/False로 변환합니다.
# 값이 'true', '1', 't', 'yes' 중 하나일 때 True가 됩니다. 기본값은 False입니다.
QDRANT_USE_HTTPS = os.getenv("QDRANT_USE_HTTPS", "False").lower() in ('true', '1', 't', 'yes')


# --- 원본 데이터베이스(MySQL) 설정 ---
# DatabaseManager 클래스가 직접 환경 변수를 참조하므로,
# 이 설정들은 main 스크립트에서 DatabaseManager를 초기화하기 전에 .env 파일에 반드시 존재해야 합니다.
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port")
DB_USERNAME = os.getenv("username")
DB_PASSWORD = os.getenv("password")
DB_NAME = os.getenv("database")


def validate_config():
    """
    스크립트 실행에 필요한 필수 환경 변수들이 모두 설정되었는지 확인하는 함수입니다.
    하나라도 누락된 경우, 오류를 발생시켜 프로그램을 중단시킵니다.
    """
    # 필수 환경 변수 목록을 딕셔너리 형태로 정의합니다.
    required_vars = {
        "OPENAI_API_KEY": OPENAI_API_KEY,
        "QDRANT_HOST": QDRANT_HOST,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_USERNAME": DB_USERNAME,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_NAME": DB_NAME,
    }
    
    # 설정되지 않은(None) 환경 변수들의 목록을 찾습니다.
    missing_vars = [key for key, value in required_vars.items() if value is None]
    
    # 누락된 변수가 있다면, 어떤 변수가 필요한지 알리는 오류 메시지와 함께 프로그램을 종료합니다.
    if missing_vars:
        raise ValueError(f"필수 환경 변수가 .env 파일에 설정되지 않았습니다: {', '.join(missing_vars)}")
    
    # 모든 필수 변수가 잘 설정되었음을 확인하는 메시지를 출력합니다.
    print("✅ 모든 필수 환경 변수가 성공적으로 로드되었습니다.")