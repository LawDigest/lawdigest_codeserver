# -*- coding: utf-8 -*-
import argparse
import sys
import os

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from src.data_operations.WorkFlowManager import WorkFlowManager

def main(start_ord: str, end_ord: str):
    """
    지정된 국회 대수 범위에 해당하는 대안-법안 관계 데이터를 수집합니다.
    """
    print(f"🚀 대안-법안 관계 데이터 수집 시작 (범위: {start_ord}대 ~ {end_ord}대)")
    
    # 'remote' 모드로 실행하여 실제 서버에 전송
    wfm = WorkFlowManager(mode='remote')
    
    # WorkFlowManager의 함수를 재사용하여 데이터 수집 및 전송
    wfm.update_bills_alternatives(start_ord=start_ord, end_ord=end_ord)
    
    print("✅ 작업이 완료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="특정 국회 대수 범위의 대안-법안 관계 데이터를 수집하는 스크립트")
    parser.add_argument("--start-ord", required=True, help="시작 국회 대수 (예: 21)")
    parser.add_argument("--end-ord", required=True, help="종료 국회 대수 (예: 21)")
    
    args = parser.parse_args()
    
    main(args.start_ord, args.end_ord)
