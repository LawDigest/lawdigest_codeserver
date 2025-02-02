import subprocess
import os
from dotenv import load_dotenv
import datetime
import shutil
import glob

# .env 파일 로드
load_dotenv()

# 현재 시간을 기준으로 덤프 파일 이름을 생성
now = datetime.datetime.now()
now_str = now.strftime('%Y%m%d%H%M%S')

# 덤프 파일이 저장될 경로
dump_directory = '/home/coder/project/dump'
dump_file_path = f'{dump_directory}/db_dump_{now_str}.sql'

# dump 디렉토리가 없으면 생성
os.makedirs(dump_directory, exist_ok=True)

# mysqldump의 절대 경로 지정
mysqldump_path = '/usr/bin/mysqldump'

# MySQL 자격 증명 (기본값 추가하여 None 방지)
db_host = os.getenv('host', 'localhost')
db_user = os.getenv('username', 'root')
db_password = os.getenv('password', '')
db_name = os.getenv('database')
db_port = os.getenv('port', '3306')

# MySQL 자격 증명이 없을 경우 예외 처리
if not db_name:
    raise ValueError("❌ 데이터베이스 이름(database)이 설정되지 않았습니다. .env 파일을 확인하세요.")

# 디렉토리 내에서 가장 오래된 덤프 파일을 찾는 함수
def get_oldest_dump_file(directory):
    list_of_files = glob.glob(f'{directory}/db_dump_*.sql')
    if not list_of_files:
        return None
    oldest_file = min(list_of_files, key=os.path.getctime)
    return oldest_file

# 디렉토리의 총 용량을 계산하는 함수
def get_directory_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

# 덤프 파일을 생성하기 전에 디렉토리의 총 용량을 확인하고, 필요시 오래된 파일을 삭제
def ensure_directory_size_within_limit(directory, max_size_gb):
    max_size_bytes = max_size_gb * (1024 ** 3)
    current_size = get_directory_size(directory)
    print(f"📌 현재 백업 디렉토리 크기: {current_size / (1024 ** 3):.2f} GB / 3.0 GB")

    while current_size > max_size_bytes:
        oldest_file = get_oldest_dump_file(directory)
        if oldest_file:
            print(f"⚠️ 용량 초과! {current_size / (1024 ** 3):.2f} GB, 가장 오래된 덤프 파일 삭제: {oldest_file}")
            os.remove(oldest_file)
            current_size = get_directory_size(directory)
        else:
            break

# 디렉토리의 용량이 3GB를 넘지 않도록 관리
ensure_directory_size_within_limit(dump_directory, 3.0)

# 환경 변수 설정하여 subprocess 실행 시 PATH 문제 방지
env = os.environ.copy()
env["PATH"] = "/usr/bin:/usr/local/bin:/bin:/sbin:" + env["PATH"]

# 덤프 파일 생성 (shell=True 방식 적용)
try:
    dump_command = (
        f"{mysqldump_path} -h {db_host} -u {db_user} --password={db_password} -P {db_port} {db_name} > {dump_file_path}"
    )

    result = subprocess.run(
        dump_command,
        shell=True,  # 쉘을 통해 실행 (환경 문제 방지)
        executable="/bin/bash",  # 명확한 실행 환경 설정
        stderr=subprocess.PIPE,
        check=True,
        env=env  # 환경 변수 적용
    )

    print(f"✅ 데이터베이스 {db_name} 백업 완료: {dump_file_path}")

except subprocess.CalledProcessError as e:
    print(f"❌ 데이터베이스 백업 중 오류 발생: {e.stderr.decode()}")
except FileNotFoundError:
    print("❌ mysqldump 명령어를 찾을 수 없습니다. mysqldump가 올바르게 설치되었는지 확인하세요.")