# -*- coding: utf-8 -*-
import argparse
import sys
import os
import traceback

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from src.data_operations.WorkFlowManager import WorkFlowManager
from src.data_operations.Notifier import Notifier

def main(start_date: str, end_date: str, age: str):
    """
    지정된 기간과 국회 대수에 해당하는 법안 데이터를 수집합니다.
    """
    notifier = Notifier()
    job_name = "법안 데이터 수집"
    args_str = f"**기간**: {start_date} ~ {end_date}, **대수**: {age}"
    start_message = f"🚀 **[{job_name}]** 작업을 시작합니다.\n- {args_str}"
    print(start_message)
    notifier.send_discord_message(start_message)

    try:
        wfm = WorkFlowManager(mode='remote')
        
        result_df = wfm.update_bills_data(start_date=start_date, end_date=end_date, age=age)
        
        data_count = len(result_df) if result_df is not None else 0
        
        success_message = f"✅ **[{job_name}]** 작업이 성공적으로 완료되었습니다.\n- {args_str}\n- **처리된 데이터**: {data_count}건"
        
        if result_df is not None and not result_df.empty and 'proposeDate' in result_df.columns:
            propose_dates_summary = result_df['proposeDate'].value_counts().sort_index().to_string()
            success_message += f"\n\n**[법안 제안일자별 분포]**\n```\n{propose_dates_summary}\n```"

        print(success_message)
        notifier.send_discord_message(success_message)

    except Exception as e:
        error_message = f"🚨 **[{job_name}]** 작업 중 오류 발생!\n- {args_str}\n\n- **오류 내용**: `{type(e).__name__}: {str(e)}`\n- **Traceback**:\n```\n{traceback.format_exc()}\n```"
        print(error_message)
        notifier.send_discord_message(error_message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="특정 기간의 법안 데이터를 수집하는 스크립트")
    parser.add_argument("--start-date", required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--age", required=True, help="국회 대수 (예: 21)")
    
    args = parser.parse_args()
    
    main(args.start_date, args.end_date, args.age)
