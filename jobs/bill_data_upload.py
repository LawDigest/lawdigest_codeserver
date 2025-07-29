import sys
import os
import datetime
import time
import traceback

# 프로젝트 루트를 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from src.data_operations import WorkFlowManager, Notifier

def main():
    """
    법안 데이터를 수집, 요약, 처리하고 서버로 전송하는 메인 함수
    """
    notifier = Notifier()
    job_name = "법안 데이터 업데이트"
    print(f"[{datetime.datetime.now()}] {job_name} 시작")
    start_time = time.time()

    try:
        # WorkFlowManager를 'remote' 모드로 실행하여 데이터 수집 및 전송
        manager = WorkFlowManager(mode='remote')
        df = manager.update_bills_data()

        end_time = time.time()
        elapsed_time = end_time - start_time
        
        success_message = (
            f"✅ **[{job_name}] 성공** ✅

"
            f"법안 데이터 업데이트 및 AI 요약, 전송이 성공적으로 완료되었습니다.
"
            f"소요 시간: {elapsed_time:.2f}초
"
        )
        
        if df is not None:
            success_message += f"처리된 법안 수: {len(df)}건"
        else:
            success_message += "새롭게 처리된 법안이 없습니다."
            
        print(success_message)
        # 성공 시에도 알림을 보내고 싶다면 아래 주석을 해제하세요.
        # notifier.send_discord_message(success_message)

    except Exception as e:
        # 에러 발생 시 알림 전송
        print(f"❌ [ERROR] {job_name} 중 에러가 발생했습니다.")
        error_traceback = traceback.format_exc()
        error_message = (
            f"🚨 **[ERROR] {job_name} 실패** 🚨

"
            f"스크립트 실행 중 다음과 같은 에러가 발생했습니다:

"
            f"```
{error_traceback}
```"
        )
        print(error_message)
        notifier.send_discord_message(error_message)

if __name__ == "__main__":
    main()