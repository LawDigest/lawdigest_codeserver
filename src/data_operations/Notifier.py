import os
import smtplib
from email.message import EmailMessage
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv


class Notifier:
    """데이터 수집 결과를 알림으로 전송하는 클래스"""

    def __init__(self) -> None:
        load_dotenv()
        self.discord_webhook = os.getenv("DISCORD_WEBHOOK_URL")
        self.email_host = os.getenv("EMAIL_HOST")
        self.email_port = int(os.getenv("EMAIL_PORT", "587"))
        self.email_user = os.getenv("EMAIL_HOST_USER")
        self.email_password = os.getenv("EMAIL_HOST_PASSWORD")
        self.email_receiver = os.getenv("EMAIL_RECEIVER")

    def _build_summary(self, df: Optional[pd.DataFrame]) -> str:
        """수집된 데이터프레임을 요약한 메시지 생성"""
        if df is None:
            return "수집된 데이터가 없습니다."
        return f"수집된 데이터 행 수: {len(df)}"

    def send_discord_message(self, df: Optional[pd.DataFrame], message: str = "") -> None:
        """Discord 웹훅으로 수집 결과 메시지를 전송"""
        if not self.discord_webhook:
            print("❌ [ERROR] DISCORD_WEBHOOK_URL 환경 변수가 설정되어 있지 않습니다.")
            return

        summary = self._build_summary(df)
        content = f"{message}\n{summary}" if message else summary
        try:
            response = requests.post(self.discord_webhook, json={"content": content})
            if response.status_code in (200, 204):
                print("✅ [INFO] Discord 메시지 전송 완료")
            else:
                print(f"❌ [ERROR] Discord 전송 실패: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ [ERROR] Discord 전송 중 예외 발생: {e}")

    def send_email(self, df: Optional[pd.DataFrame], subject: str = "데이터 수집 결과", message: str = "") -> None:
        """메일로 수집 결과 메시지를 전송"""
        if not all([self.email_host, self.email_user, self.email_password, self.email_receiver]):
            print("❌ [ERROR] 메일 환경 변수가 올바르게 설정되어 있지 않습니다.")
            return

        summary = self._build_summary(df)
        body = f"{message}\n{summary}" if message else summary

        email = EmailMessage()
        email["Subject"] = subject
        email["From"] = self.email_user
        email["To"] = self.email_receiver
        email.set_content(body)

        try:
            with smtplib.SMTP(self.email_host, self.email_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(email)
            print("✅ [INFO] 이메일 전송 완료")
        except Exception as e:
            print(f"❌ [ERROR] 이메일 전송 중 예외 발생: {e}")

