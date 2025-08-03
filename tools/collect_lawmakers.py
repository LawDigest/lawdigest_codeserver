# -*- coding: utf-8 -*-
import argparse
import sys
import os

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from src.data_operations.WorkFlowManager import WorkFlowManager

def main():
    """
    전체 국회의원 데이터를 수집합니다.
    """
    print("🚀 전체 의원 데이터 수집 시작")
    
    # 'remote' 모드로 실행하여 실제 서버에 전송
    wfm = WorkFlowManager(mode='remote')
    
    # WorkFlowManager의 함수를 재사용하여 데이터 수집 및 전송
    wfm.update_lawmakers_data()
    
    print("✅ 작업이 완료되었습니다.")

if __name__ == "__main__":
    # 이 스크립트는 별도의 인자를 받지 않습니다.
    parser = argparse.ArgumentParser(description="전체 국회의원 데이터를 수집하는 스크립트")
    parser.parse_args()
    
    main()
