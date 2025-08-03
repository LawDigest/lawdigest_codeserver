#!/bin/bash

# 이 스크립트는 데이터 무결성을 위해 법안 관련 데이터 업데이트 스크립트를 순차적으로 실행합니다.
# 각 스크립트는 성공적으로 완료되어야 다음 스크립트가 실행됩니다 (&& 연산자 사용).

# 프로젝트 루트 디렉토리로 이동
cd "$(dirname "$0")/.."

LOG_DIR="log"
MASTER_LOG_FILE="$LOG_DIR/sequential_updates.log"

# 마스터 로그 시작
echo "============================================================" >> "$MASTER_LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 순차 데이터 업데이트 작업을 시작합니다." >> "$MASTER_LOG_FILE"

# 1. 의원 정보 업데이트
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 1. 의원 정보 업데이트 시작" >> "$MASTER_LOG_FILE"
/home/ubuntu/project/lawdigest/scripts/run_update_lawmakers.sh && \
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 1. 의원 정보 업데이트 성공" >> "$MASTER_LOG_FILE" && \

# 2. 법안 정보 업데이트
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 2. 법안 정보 업데이트 시작" >> "$MASTER_LOG_FILE"
/home/ubuntu/project/lawdigest/scripts/run_update_bills.sh && \
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 2. 법안 정보 업데이트 성공" >> "$MASTER_LOG_FILE" && \

# 3. 법안 타임라인 업데이트
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 3. 법안 타임라인 업데이트 시작" >> "$MASTER_LOG_FILE"
/home/ubuntu/project/lawdigest/scripts/run_update_timeline.sh && \
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 3. 법안 타임라인 업데이트 성공" >> "$MASTER_LOG_FILE" && \

# 4. 법안 표결 정보 업데이트
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 4. 법안 표결 정보 업데이트 시작" >> "$MASTER_LOG_FILE"
/home/ubuntu/project/lawdigest/scripts/run_update_votes.sh && \
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 4. 법안 표결 정보 업데이트 성공" >> "$MASTER_LOG_FILE" && \

# 5. 법안 처리 결과 업데이트
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 5. 법안 처리 결과 업데이트 시작" >> "$MASTER_LOG_FILE"
/home/ubuntu/project/lawdigest/scripts/run_update_results.sh && \
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 5. 법안 처리 결과 업데이트 성공" >> "$MASTER_LOG_FILE"

RESULT=$?
if [ $RESULT -eq 0 ]; then
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 모든 순차 데이터 업데이트 작업이 성공적으로 완료되었습니다." >> "$MASTER_LOG_FILE"
else
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 순차 데이터 업데이트 작업 중 오류가 발생했습니다. (종료 코드: $RESULT)" >> "$MASTER_LOG_FILE"
fi

echo "============================================================" >> "$MASTER_LOG_FILE"
