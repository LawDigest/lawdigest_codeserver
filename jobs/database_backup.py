import subprocess
import os
from dotenv import load_dotenv
import datetime
import glob

# .env 파일에서 환경 변수 로드
load_dotenv()

# --- 설정 변수 ---
# 백업 파일을 저장할 디렉토리 (프로젝트 루트/backup)
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'dump')
MAX_BACKUP_SIZE_GB = 3.0 # 백업 디렉토리의 최대 크기 (GB)

# mysqldump 명령어 경로 (환경에 따라 다를 수 있음)
MysqlDUMP_PATH = '/usr/bin/mysqldump'

# DB 연결 정보
DB_HOST = os.getenv('host', 'localhost')
DB_USER = os.getenv('username', 'root')
DB_PASSWORD = os.getenv('password', '')
DB_NAME = os.getenv('database')
DB_PORT = os.getenv('port', '3306')

def get_directory_size(directory):
    """디렉토리의 전체 크기를 바이트 단위로 반환합니다."""
    total_size = 0
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # 심볼릭 링크가 아닐 경우에만 크기 계산
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def get_oldest_dump_file(directory):
    """디렉토리에서 가장 오래된 덤프 파일을 찾습니다."""
    list_of_files = glob.glob(f'{directory}/db_dump_*.sql')
    if not list_of_files:
        return None
    return min(list_of_files, key=os.path.getctime)

def ensure_directory_size_limit():
    """백업 디렉토리의 크기를 확인하고, 최대 크기를 초과하면 오래된 파일을 삭제합니다."""
    max_size_bytes = MAX_BACKUP_SIZE_GB * (1024 ** 3)
    current_size = get_directory_size(BACKUP_DIR)
    print(f"📌 현재 백업 디렉토리 크기: {current_size / (1024 ** 3):.2f} GB / {MAX_BACKUP_SIZE_GB} GB")

    while current_size > max_size_bytes:
        oldest_file = get_oldest_dump_file(BACKUP_DIR)
        if oldest_file:
            print(f"⚠️ 용량 초과! 가장 오래된 덤프 파일 삭제: {oldest_file}")
            os.remove(oldest_file)
            current_size = get_directory_size(BACKUP_DIR)
        else:
            print("삭제할 덤프 파일이 없습니다.")
            break

def main():
    """데이터베이스 백업을 실행하는 메인 함수"""
    print(f"[{datetime.datetime.now()}] 데이터베이스 백업 시작")

    if not DB_NAME:
        print("❌ [ERROR] .env 파일에 데이터베이스 이름(database)이 설정되지 않았습니다.")
        return

    # 백업 디렉토리 생성
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # 용량 관리
    ensure_directory_size_limit()

    # 백업 파일 경로 설정
    now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    dump_file_path = os.path.join(BACKUP_DIR, f'db_dump_{now_str}.sql')

    # mysqldump 명령어 생성
    dump_command = (
        f'{MysqlDUMP_PATH} -h {DB_HOST} -P {DB_PORT} -u {DB_USER} --password={DB_PASSWORD} \
        --single-transaction --routines --triggers --events --no-create-db --skip-opt \
        {DB_NAME} > {dump_file_path}'
    )

    try:
        # 셸을 통해 명령어 실행
        subprocess.run(
            dump_command,
            shell=True,
            check=True,
            stderr=subprocess.PIPE,
            executable="/bin/bash"
        )
        print(f"✅ 데이터베이스 '{DB_NAME}' 백업 완료: {dump_file_path}")

    except subprocess.CalledProcessError as e:
        print(f"❌ [ERROR] 데이터베이스 백업 중 오류 발생: {e.stderr.decode()}")
    except FileNotFoundError:
        print(f"❌ [ERROR] '{MysqlDUMP_PATH}' 명령어를 찾을 수 없습니다. 경로를 확인하세요.")

if __name__ == "__main__":
    main()
