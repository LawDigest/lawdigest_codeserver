import data_operations as dataops
import datetime
import time
import traceback
from data_operations import Notifier # Notifier 클래스를 import 합니다.

# 1. Notifier 인스턴스 생성
notifier = Notifier()

# 오늘 날짜를 출력
print(f'{datetime.datetime.now()} 법안 데이터 업데이트 시작')
start_time = time.time()

try:
    # 2. 데이터 업데이트 로직 실행
    manager = dataops.WorkFlowManager(mode='remote')
    df = manager.update_bills_data()

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f'{datetime.datetime.now()} 법안 데이터 업데이트 완료')
    print(f'소요시간: {elapsed_time:.2f}초')

except Exception as e:
    # 4. 에러 발생 시 알림 전송
    print(f"❌ [ERROR] 스크립트 실행 중 에러가 발생했습니다.")
    
    # 에러의 상세 내용을 traceback을 이용해 문자열로 만듭니다.
    error_traceback = traceback.format_exc()
    
    # 디스코드로 보낼 에러 메시지를 구성합니다.
    error_message = (
        f"🚨 **[ERROR] 법안 데이터 업데이트 실패** 🚨\n\n"
        f"스크립트 실행 중 다음과 같은 에러가 발생했습니다:\n\n"
        f"```\n{error_traceback}\n```"
    )
    
    # 디스코드로 에러 메시지 전송
    notifier.send_discord_message(error_message)
    
    # 콘솔에도 에러 내용을 출력합니다.
    print(error_traceback)
