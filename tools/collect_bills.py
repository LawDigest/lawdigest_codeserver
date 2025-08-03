# -*- coding: utf-8 -*-
import argparse
import sys
import os

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from src.data_operations.WorkFlowManager import WorkFlowManager

def main(start_date: str, end_date: str, age: str):
    """
    지정된 기간과 국회 대수에 해당하는 법안 데이터를 수집합니다.
    """
    print(f"🚀 법안 데이터 수집 시작 (기간: {start_date} ~ {end_date}, 대수: {age})")
    
    # 'remote' 모드로 실행하여 실제 서버에 전송
    wfm = WorkFlowManager(mode='remote')
    
    # WorkFlowManager의 함수를 재사용하여 데이터 수집 및 전송
    wfm.update_bills_data(start_date=start_date, end_date=end_date, age=age)
    
    print("✅ 작업이 완료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="특정 기간의 법안 데이터를 수집하는 스크립트")
    parser.add_argument("--start-date", required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--age", required=True, help="국회 대수 (예: 21)")
    
    args = parser.parse_args()
    
    main(args.start_date, args.end_date, args.age)
