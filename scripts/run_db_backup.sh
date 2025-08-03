#!/bin/bash

# 프로젝트 루트 디렉토리로 이동
cd "$(dirname "$0")/.."

# 가상환경 활성화
source ./lawdigestenv/bin/activate

# 로그 디렉토리 생성
LOG_DIR="log"
mkdir -p $LOG_DIR

# 로그 파일 경로 설정
LOG_FILE="$LOG_DIR/db_backup_$(date +'%Y%m%d_%H%M%S').log"

# 파이썬 스크립트 실행 및 로그 기록
echo "데이터베이스 백업 작업을 시작합니다. 로그 파일: $LOG_FILE"
python3 ./jobs/database_backup.py > "$LOG_FILE" 2>&1

echo "작업 완료. 결과는 로그 파일을 확인하세요."
