import requests
import pandas as pd

class APISender:
    def __init__(self):
        self.post_url = None


    def request_post(self, url=None):

        if url == None:
            print("URL을 입력해주세요.")
            return None
        
        try:
            response = requests.post(url)

            # 응답 확인
            if response.status_code == 200:
                print(f'서버 요청 성공: {url}')
                print('응답 데이터:', response.json())
            else:
                print(f'서버 요청 실패: {url}')
                print('상태 코드:', response.status_code)
                print('응답 내용:', response.text)
            
            response.raise_for_status() # 200번대 코드가 아니면 예외를 발생시킴
            return response 
        except Exception as e:
            print(f"서버 요청 중 오류 발생: {e}")

    def send_data(self, data, url, payload_name):
        """
        데이터를 JSON 형식으로 변환하여 API 서버로 전송하는 함수.

        Parameters:
        - data: pandas.DataFrame 또는 dict, 전송할 데이터
        - payload_name: str, payload의 이름 (예: "lawmakerDfRequestList")
        - url: str, 데이터를 전송할 API 엔드포인트 URL

        Returns:
        - response: requests.Response, API 서버로부터 받은 응답 객체
        """
        if isinstance(data, pd.DataFrame):
            # DataFrame을 JSON 형식으로 변환
            data = data.to_dict(orient='records')
        
        # payload 생성
        payload = {payload_name: data}
        
        # 헤더 설정
        headers = {
            'Content-Type': 'application/json',
        }

        # POST 요청 보내기
        try:
            response = requests.post(url, headers=headers, json=payload)

            # 응답 확인
            if response.status_code == 200:
                print(f'데이터 전송 성공: {url}')
                print('응답 데이터:', response.json())
            else:
                print(f'데이터 전송 실패: {url}')
                print('상태 코드:', response.status_code)
                print('응답 내용:', response.text)
            
            response.raise_for_status() # 200번대 코드가 아니면 예외를 발생시킴
            return response
        except Exception as e:
            print(f"데이터 전송 중 오류 발생: {e}")
            raise # 예외를 다시 발생시켜 호출자에게 전파
