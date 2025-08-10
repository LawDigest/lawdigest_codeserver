# -*- coding: utf-8 -*-
import argparse
import sys
import os
import traceback

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from src.data_operations.WorkFlowManager import WorkFlowManager
from src.data_operations.Notifier import Notifier

def main():
    """
    전체 국회의원 데이터를 수집합니다.
    """
    notifier = Notifier()
    job_name = "전체 의원 데이터 수집"
    start_message = f"🚀 **[{job_name}]** 작업을 시작합니다."
    print(start_message)
    notifier.send_discord_message(start_message)

    try:
        wfm = WorkFlowManager(mode='remote')
        
        result_df = wfm.update_lawmakers_data()
        
        data_count = len(result_df) if result_df is not None else 0
        
        success_message = f"✅ **[{job_name}]** 작업이 성공적으로 완료되었습니다.\n- **처리된 데이터**: {data_count}건"
        print(success_message)
        notifier.send_discord_message(success_message)

    except Exception as e:
        error_message = f"🚨 **[{job_name}]** 작업 중 오류 발생!\n\n- **오류 내용**: `{type(e).__name__}: {str(e)}`\n- **Traceback**:\n```\n{traceback.format_exc()}\n```"
        print(error_message)
        notifier.send_discord_message(error_message)

if __name__ == "__main__":
    # 이 스크립트는 별도의 인자를 받지 않습니다.
    parser = argparse.ArgumentParser(description="전체 국회의원 데이터를 수집하는 스크립트")
    parser.parse_args()
    
    main()
