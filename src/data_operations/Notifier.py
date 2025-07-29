import os
import smtplib
import io
from email.message import EmailMessage
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv


class Notifier:
    """데이터 수집 결과를 다양한 채널로 알림을 전송하는 클래스"""

    def __init__(self) -> None:
        """환경 변수에서 설정 값을 로드하여 초기화합니다."""
        load_dotenv()
        self.discord_webhook: Optional[str] = os.getenv("DISCORD_WEBHOOK_URL")
        # self.email_host: Optional[str] = os.getenv("EMAIL_HOST")
        # self.email_port: int = int(os.getenv("EMAIL_PORT", "587"))
        # self.email_user: Optional[str] = os.getenv("EMAIL_HOST_USER")
        # self.email_password: Optional[str] = os.getenv("EMAIL_HOST_PASSWORD")
        # self.email_receiver: Optional[str] = os.getenv("EMAIL_RECEIVER")
        print("✅ [INFO] Notifier가 초기화되었습니다.")

    def _build_message(self, subject: str, data: pd.DataFrame) -> str:
        """
        subject와 데이터프레임을 기반으로 핵심 요약 메시지를 생성합니다.
        
        Args:
            subject (str): 데이터의 종류를 나타내는 문자열 (예: "bills", "lawmakers").
            data (pd.DataFrame): 수집된 데이터가 담긴 데이터프레임.

        Returns:
            str: 생성된 요약 메시지.
        """
        # --- 알림 시각 (한국 표준시 기준) ---
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')
        timestamp = f"🕒 **[알림 시각: {now}]**"

        if data.empty:
            return f"{timestamp}\n\n✅ **[{subject.upper()}]**\n수집된 데이터가 없습니다."

        # --- 기본 정보 (모든 데이터에 공통) ---
        title = f"✅ **[{subject.upper()} 데이터 수집 결과]**"
        total_rows = f"총 **{len(data):,}** 건의 데이터가 수집되었습니다."
        
        # --- Subject별 특화 정보 ---
        specific_info = ""
        match subject:
            case "bills":
                propose_dates = data['proposeDate'].value_counts().sort_index().to_string()
                proposer_kind = data['proposerKind'].value_counts().sort_index().to_string()
                specific_info = f"""
                **[법안 제안일자별 분포]**\n```\n{propose_dates}\n```
                **[법안 발의주체별 분포]**\n```\n{proposer_kind}\n```
                """
            case "bill_coactors":
                pass
            case "lawmakers":
                pass
            case "bill_timeline" | "bill_result" | "bill_vote" | "vote_party" | "alternative_bill":
                pass
            case _:
                pass
        
        # --- 최종 메시지 조합 ---
        message_parts = [
            title,
            total_rows,
            timestamp,
            specific_info
        ]
        
        return "\n\n".join(part for part in message_parts if part)

    def notify(self, subject: str, data: Optional[pd.DataFrame], custom_message: str = "") -> None:
        """
        주어진 subject와 데이터에 따라 적절한 알림을 생성하고 전송합니다.

        Args:
            subject (str): 데이터의 종류. 이 값에 따라 메시지 내용이 달라집니다.
            data (Optional[pd.DataFrame]): 수집된 데이터. None이거나 비어있을 수 있습니다.
            custom_message (str, optional): 메시지 끝에 추가할 사용자 정의 문자열.
        """
        print(f"🚀 [INFO] '{subject}' 주제로 알림 전송을 시작합니다.")
        
        if data is None:
            data = pd.DataFrame() # 빈 데이터프레임으로 처리 통일

        # 1. 핵심 메시지 생성
        final_message = self._build_message(subject, data)

        # 2. 사용자 지정 메시지 추가
        if custom_message:
            final_message += f"\n\n**[추가 메시지]**\n{custom_message}"

        # 이메일 제목 설정
        email_subject = f"[{subject.upper()}] 데이터 수집 결과 알림"

        # 3. 각 채널로 알림 전송
        self.send_discord_message(final_message)
        # self.send_email(subject=email_subject, body=final_message)

    def send_discord_message(self, content: str) -> None:
        """
        Discord 웹훅으로 메시지를 전송합니다.

        Args:
            content (str): 전송할 메시지 내용.
        """
        if not self.discord_webhook:
            print("⚠️ [WARN] DISCORD_WEBHOOK_URL 환경 변수가 설정되어 있지 않아 Discord 메시지를 건너뜁니다.")
            return

        # Discord 메시지 길이 제한 (2000자) 처리
        if len(content) > 2000:
            content = content[:1997] + "..."
            print("⚠️ [WARN] Discord 메시지가 너무 길어 일부를 잘라내어 전송합니다.")

        try:
            response = requests.post(self.discord_webhook, json={"content": content})
            if response.status_code in (200, 204):
                print("✅ [INFO] Discord 메시지 전송 완료")
            else:
                print(f"❌ [ERROR] Discord 전송 실패: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ [ERROR] Discord 전송 중 예외 발생: {e}")

    # def send_email(self, subject: str, body: str) -> None:
    #     ...

# --- 사용 예시 ---
if __name__ == '__main__':
    # .env 파일이 현재 디렉토리에 있다고 가정하고 테스트를 위해 설정합니다.
    # 실제 사용 시에는 환경 변수를 직접 설정해야 합니다.
    # 예: os.environ['DISCORD_WEBHOOK_URL'] = 'your_webhook_url'
    
    # 가상 데이터프레임 생성
    bill_data = {
        'billId': [f'B{i}' for i in range(10)],
        'proposeDate': pd.to_datetime(['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-03'] * 2 + ['2023-01-04'] * 2),
        'summary': ['Test summary'] * 10,
        'age': [21] * 10
    }
    bills_df = pd.DataFrame(bill_data)

    lawmaker_data = {
        'lawmakerId': [f'L{i}' for i in range(5)],
        'lawmakerName': ['김의원', '이의원', '박의원', '최의원', '정의원'],
        'polyNm': ['국민의힘', '더불어민주당', '국민의힘', '더불어민주당', '정의당']
    }
    lawmakers_df = pd.DataFrame(lawmaker_data)
    
    empty_df = pd.DataFrame()

    # Notifier 인스턴스 생성
    notifier = Notifier()

    print("\n--- 'bills' 주제 테스트 ---")
    notifier.notify(
        subject="bills", 
        data=bills_df, 
        custom_message="정기 데이터 수집이 성공적으로 완료되었습니다."
    )
    
    print("\n--- 'lawmakers' 주제 테스트 ---")
    notifier.notify(
        subject="lawmakers",
        data=lawmakers_df
    )

    print("\n--- 빈 데이터프레임 테스트 ---")
    notifier.notify(
        subject="bill_vote",
        data=empty_df,
        custom_message="오늘 수집된 투표 데이터가 없습니다."
    )
