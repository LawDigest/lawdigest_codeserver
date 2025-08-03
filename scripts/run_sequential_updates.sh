#!/bin/bash

# 프로젝트 루트 디렉토리로 이동
cd "$(dirname "$0")/.."

# 가상환경 활성화
source ./lawdigestenv/bin/activate

# 로그 디렉토리 생성
LOG_DIR="log"
mkdir -p $LOG_DIR

# 로그 파일 경로 설정
LOG_FILE="$LOG_DIR/hourly_update_$(date +'%Y%m%d_%H%M%S').log"

# 새로운 마스터 파이썬 스크립트 실행
echo "시간별 데이터 업데이트 및 리포팅 작업을 시작합니다. 로그 파일: $LOG_FILE"
python3 -u ./jobs/hourly_data_update.py 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -ne 0 ]; then
    echo "시간별 데이터 업데이트 작업 실패. 종료 코드: $EXIT_CODE"
    exit $EXIT_CODE
fi

echo "모든 작업이 성공적으로 완료되었습니다. 자세한 내용은 로그 파일을 확인하세요."
