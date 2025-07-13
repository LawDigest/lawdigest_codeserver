import requests
import pandas as pd
from xml.etree import ElementTree
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from .DataFetcher import DataFetcher
from .DataProcessor import DataProcessor
from .AISummarizer import AISummarizer
from .APISender import APISender
from .DatabaseManager import DatabaseManager
from .Notifier import Notifier

class WorkFlowManager:
    def __init__(self, mode):
        """Workflow manager initialization

        Parameters
        ----------
        mode: str, optional
            실행 모드. remote | local | test | save | fetch 중 하나.
        """
        self.mode_list = ['remote', 'local', 'test', 'save', 'fetch', 'ai_test']
        if mode not in self.mode_list:
            raise ValueError(
                f"올바른 모드를 선택해주세요. {self.mode_list}"
            )
        self.mode = mode

        load_dotenv()

    def update_bills_data(self, start_date=None, end_date=None, age=None):
        """법안 데이터를 수집해 AI 요약 후 API 서버로 전송하는 함수

        Args:
            start_date (str, optional): 시작 날짜 (YYYY-MM-DD 형식). Defaults to None.
            end_date (str, optional): 종료 날짜 (YYYY-MM-DD 형식). Defaults to None.
            age (str, optional): 국회 데이터 수집 대수
            
        Note:
            실행 모드는 클래스 생성 시 설정한 ``self.mode`` 값을 사용합니다.

        Returns:
            pd.DataFrame: 전송된 데이터프레임
        """
        print("[법안 데이터 수집 및 전송 시작]")

        mode = self.mode
        
        # 데이터 수집 기간 설정
        if start_date is None:
            # DB에 연결하여 현재 가장 최신 법안 날짜 가져오기
            try:
                DBconn = DatabaseManager()
                latest_propose_dt = DBconn.get_latest_propose_date()

                #DB에서 최신 법안 날짜 가져오는데 실패한 경우
                if latest_propose_dt is None:
                    raise ValueError("DB에서 최신 법안 날짜를 가져올 수 없습니다. 데이터가 비어있을 수 있습니다.")

                start_date = latest_propose_dt

            # DB 연결이나 쿼리 자체에서 오류가 발생한 경우
            except Exception as e:
                # 원본 에러(e)를 포함하여 새로운 에러를 발생시키면 디버깅에 용이합니다.
                raise ConnectionError(f"데이터베이스 조회 중 오류가 발생했습니다: {e}")

        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if age is None:
            age = os.getenv("AGE")

        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'age': age
        }
        
        # 1. 데이터 받아오기
        fetcher = DataFetcher(params)

        df_bills = fetcher.fetch_data('bills')
        # bills_info_data = fetcher.fetch_data('bill_info')

        # 2. 데이터 처리
        
        # 데이터 처리 객체 선언
        processor = DataProcessor(fetcher)

        # 법안 데이터 머지
        # df_bills = processor.merge_bills_df(bills_content_data, bills_info_data)

        # 중복 데이터 제거 (fetch 모드에서는 수행하지 않음)
        if mode != 'fetch':
            processor.remove_duplicates(df_bills, DatabaseManager())

        if len(df_bills) == 0:
            print("새로운 데이터가 없습니다. 코드를 종료합니다.")
            return None

        # AI 요약 컬럼 추가
        processor.add_AI_summary_columns(df_bills)

        # 의원 데이터 처리
        df_bills_congressman = processor.process_congressman_bills(df_bills)

        # 위원장 데이터 처리
        df_bills_chair, df_alternatives = processor.process_chairman_bills(df_bills)

        # 정부 데이터 처리
        df_bills_gov = processor.process_gov_bills(df_bills)

        # 발의주체별 법안 데이터 합치기
        df_bills = pd.concat([df_bills_congressman, df_bills_chair, df_bills_gov], ignore_index=True)

        # 데이터 전송을 위한 commitee 컬럼 임의 추가
        # TODO: 민준님이 백엔드단에서 필요 컬럼 수정 작업하고 나면 이 부분 삭제할 것
        df_bills['commitee'] = None

        # 3. 데이터 AI 요약 및 전송(모드별 처리)
        payload_name = os.environ.get("PAYLOAD_bills")
        url = os.environ.get("POST_URL_bills")

        summerizer = AISummarizer()
        sender = APISender()

        if mode == 'remote':
            print("[데이터 요약 및 전송 시작]")

            # 제목 요약
            summerizer.AI_title_summarize(df_bills)

            # 내용 요약
            summerizer.AI_content_summarize(df_bills)

            # 데이터 전송
            sender.send_data(df_bills, url, payload_name)

            print("[정당별 법안 발의수 갱신 요청 중...]")
            post_url_party_bill_count = os.environ.get("POST_URL_party_bill_count")
            sender.request_post(post_url_party_bill_count)
            print("[정당별 법안 발의수 갱신 요청 완료]")

            print("[의원별 최신 발의날짜 갱신 요청 중...]")
            post_ulr_congressman_propose_date = os.environ.get("POST_URL_congressman_propose_date")
            sender.request_post(post_ulr_congressman_propose_date)
            print("[의원별 최신 발의날짜 갱신 요청 완료]")

                # Notifier 인스턴스 생성
            notifier = Notifier()

            print("\n--- 'bills' 주제 테스트 ---")
            notifier.notify(
                subject="bills", 
                data=df_bills, 
            )

        elif mode == 'local':
            print("[로컬 모드 : AI 요약 생략 및 로컬 DB에 전송]")
            df_bills['briefSummary'] = ""
            df_bills['gptSummary'] = ""
            url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
            sender.send_data(df_bills, url, payload_name)

        elif mode == 'test':
            print('[테스트 모드 : 데이터 요약 및 전송 생략]')

        elif mode == 'save':
            df_bills.to_csv('df_bills.csv', index=False)
            print('[데이터 저장 완료]')

        elif mode == 'fetch':
            print('[데이터 수집 모드: 중복 데이터 제거 없이 데이터를 수집합니다.]')

        elif mode == 'ai_test':
            
            print("[AI 요약 테스트 모드: 5개의 법안 AI 요약을 수행합니다.]")
            print("[데이터 요약 시작]")

            df_bills = df_bills[:5]

            # 제목 요약
            summerizer.AI_title_summarize(df_bills)

            # 내용 요약
            summerizer.AI_content_summarize(df_bills)

        return df_bills



    def update_lawmakers_data(self):
        """국회의원 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        print("\n[의원 데이터 수집 시작]")

        # 데이터 수집
        fetcher = DataFetcher(params=None)
        df_lawmakers = fetcher.fetch_data('lawmakers')

        if df_lawmakers is None or df_lawmakers.empty:
            print("❌ [ERROR] 수집된 의원 데이터가 없습니다.")
            return None

        # 필요 없는 컬럼 제거
        columns_to_drop = [
            'ENG_NM',       # 영문이름
            'HJ_NM',        # 한자이름
            'BTH_GBN_NM',   # 음력/양력 구분
            'ELECT_GBN_NM', # 선거구 구분(지역구/비례)
            'STAFF',        # 보좌관
            'CMITS',        # 소속위원회 목록
            'SECRETARY',    # 비서관
            'SECRETARY2',   # 비서
            'JOB_RES_NM',   # 직위
        ]

        df_lawmakers = df_lawmakers.drop(columns=columns_to_drop)

        # UNITS 컬럼에서 숫자만 추출하여 대수 정보로 사용
        df_lawmakers['UNITS'] = df_lawmakers['UNITS'].str.extract(r'(\d+)(?=\D*$)').astype(int)

        # 컬럼명 매핑
        column_mapping = {
            'MONA_CD': 'congressmanId',
            'HG_NM': 'congressmanName',
            'CMIT_NM': 'commits',
            'POLY_NM': 'partyName',
            'REELE_GBN_NM': 'elected',
            'HOMEPAGE': 'homepage',
            'ORIG_NM': 'district',
            'UNITS': 'assemblyNumber',
            'BTH_DATE': 'congressmanBirth',
            'SEX_GBN_NM': 'sex',
            'E_MAIL': 'email',
            'ASSEM_ADDR': 'congressmanOffice',
            'TEL_NO': 'congressmanTelephone',
            'MEM_TITLE': 'briefHistory',
        }

        df_lawmakers.rename(columns=column_mapping, inplace=True)

        # 모드별 처리
        payload_name = os.getenv("PAYLOAD_lawmakers")
        url = os.getenv("POST_URL_lawmakers")

        sender = APISender()

        mode = self.mode

        if mode == 'remote':
            sender.send_data(df_lawmakers, url, payload_name)

            print("[정당별 의원수 갱신 요청 중...]")
            post_url_party_bill_count = os.environ.get("POST_URL_party_bill_count")
            sender.request_post(post_url_party_bill_count)
            print("[정당별 의원수 갱신 요청 완료]")

        elif mode == 'local':
            url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
            sender.send_data(df_lawmakers, url, payload_name)
        
        elif mode == 'test':
            print("[테스트 모드 : DB에 데이터를 전송하지 않습니다.]")

        elif mode == 'save':
            df_lawmakers.to_csv('df_lawmakers.csv', index=False)

        return df_lawmakers

    def update_bills_timeline(self, start_date=None, end_date=None, age=None):
        """의정활동(법안 처리 단계) 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        # 기본 날짜 설정: DB에 저장된 최신 날짜 다음 날부터 오늘까지
        if start_date is None:
            DBconn = DatabaseManager()
            latest_date = DBconn.get_latest_timeline_date()
            start_date = latest_date.strftime('%Y-%m-%d') if latest_date else (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if age is None:
            age = os.getenv('AGE')

        params = {
            'start_date': start_date,
            'end_date': end_date,
            'age': age
        }

        # 데이터 수집
        fetcher = DataFetcher(params)
        df_stage = fetcher.fetch_data('bill_timeline')

        if df_stage is None or df_stage.empty:
            print("❌ [ERROR] 수집된 데이터가 없습니다.")
            return None

        # 필요한 컬럼만 선택
        df_stage = df_stage[['DT', 'BILL_ID', 'STAGE', 'COMMITTEE']]

        # 컬럼명 매핑
        column_mapping = {
            'DT': 'statusUpdateDate',
            'BILL_ID': 'billId',
            'STAGE': 'stage',
            'COMMITTEE': 'committee',
        }
        df_stage.rename(columns=column_mapping, inplace=True)

        print("데이터 개수 : ", len(df_stage))

        mode = self.mode

        payload_name = os.getenv('PAYLOAD_status')
        url = os.getenv('POST_URL_status')

        sender = APISender()

        if mode == 'remote':
            total_rows = len(df_stage)
            chunks = [df_stage[i:i + 1000] for i in range(0, total_rows, 1000)]
            total_chunks = len(chunks)
            successful_chunks = 0
            failed_chunks = 0
            not_found_bill_count = 0

            for i, chunk in enumerate(chunks, 1):
                print(f"[청크 {i}/{total_chunks} 처리 중 (진행률: {i/total_chunks*100:.2f}%)]")
                try:
                    response = sender.send_data(chunk, url, payload_name)
                    response = response.json()
                    not_found_bill_count += len(response['data']['notFoundBill'])
                    print(f"[청크 {i} 데이터 전송 완료 (진행률: {i/total_chunks*100:.2f}%)]")
                    successful_chunks += 1
                except Exception as e:
                    print(f"[청크 {i} 처리 중 오류 발생: {e} (진행률: {i/total_chunks*100:.2f}%)]")
                    failed_chunks += 1

            print("[데이터 전송 완료]")
            print(f"전송 성공한 청크: {successful_chunks} / 전체 청크: {total_chunks} (성공률: {successful_chunks/total_chunks*100:.2f}%)")
            print(f"전송 실패한 청크: {failed_chunks} (실패율: {failed_chunks/total_chunks*100:.2f}%)")
            print(f"총 notFoundBill 항목의 개수: {not_found_bill_count}")

        elif mode == 'local':
            url = url.replace('https://api.lawdigest.net', 'http://localhost:8080')
            print(f'[로컬 모드 : {url}로 데이터 전송]')
            sender.send_data(df_stage, url, payload_name)

        elif mode == 'test':
            print('[테스트 모드 : 데이터 전송 생략]')

        elif mode == 'save':
            df_stage.to_csv('bills_status.csv', index=False)
            print('[데이터 저장 완료]')

        return df_stage

    def update_bills_result(self, start_date=None, end_date=None, age=None):
        """법안 처리 결과 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if age is None:
            age = os.getenv('AGE')

        params = {
            'start_date': start_date,
            'end_date': end_date,
            'age': age
        }

        fetcher = DataFetcher(params)
        df_result = fetcher.fetch_data('bill_result')

        if df_result is None or df_result.empty:
            print("❌ [ERROR] 수집된 데이터가 없습니다.")
            return None

        df_result = df_result[['BILL_ID', 'PROC_RESULT_CD']]

        column_mapping = {
            'BILL_ID': 'billId',
            'PROC_RESULT_CD': 'billProposeResult'
        }

        df_result.rename(columns=column_mapping, inplace=True)

        print("데이터 개수 : ", len(df_result))

        mode = self.mode

        payload_name = os.getenv('PAYLOAD_result')
        url = os.getenv('POST_URL_result')

        sender = APISender()

        if mode == 'remote':
            total_rows = len(df_result)
            chunks = [df_result[i:i + 1000] for i in range(0, total_rows, 1000)]
            total_chunks = len(chunks)

            for i, chunk in enumerate(chunks, 1):
                print(f"[청크 {i}/{total_chunks} 전송 중]")
                sender.send_data(chunk, url, payload_name)
                print(f"[청크 {i} 전송 완료]")

        elif mode == 'local':
            url = url.replace('https://api.lawdigest.net', 'http://localhost:8080')
            print(f'[로컬 모드 : {url}로 데이터 전송]')
            sender.send_data(df_result, url, payload_name)

        elif mode == 'test':
            print('[테스트 모드 : 데이터 전송 생략]')

        elif mode == 'save':
            df_result.to_csv('bills_result.csv', index=False)
            print('[데이터 저장 완료]')

        return df_result

    def update_bills_vote(self, start_date=None, end_date=None, age=None):
        """본회의 표결 결과 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if age is None:
            age = os.getenv('AGE')

        params = {
            'start_date': start_date,
            'end_date': end_date,
            'age': age
        }

        fetcher = DataFetcher(params)
        df_vote = fetcher.fetch_data('bill_vote')

        if df_vote is None or df_vote.empty:
            print("❌ [ERROR] 수집된 표결 결과 데이터가 없습니다.")
            return None

        df_vote_party = fetcher.fetch_data('vote_party')

        columns_to_keep = [
            'BILL_ID',
            'VOTE_TCNT',
            'YES_TCNT',
            'NO_TCNT',
            'BLANK_TCNT'
        ]

        df_vote = df_vote[columns_to_keep]
        df_vote.dropna(subset=['VOTE_TCNT'], inplace=True)
        df_vote.fillna(0, inplace=True)

        column_mapping = {
            'BILL_ID': 'billId',
            'VOTE_TCNT': 'totalVoteCount',
            'YES_TCNT': 'voteForCount',
            'NO_TCNT': 'voteAgainstCount',
            'BLANK_TCNT': 'abstentionCount'
        }

        df_vote.rename(columns=column_mapping, inplace=True)

        mode = self.mode

        payload_vote = os.getenv('PAYLOAD_vote')
        url_vote = os.getenv('POST_URL_vote')

        sender = APISender()

        if mode == 'remote':
            total_chunks = len(df_vote) // 1000 + (1 if len(df_vote) % 1000 > 0 else 0)
            for i in range(0, len(df_vote), 1000):
                df_chunk = df_vote.iloc[i:i + 1000]
                print(f"[표결 데이터 청크 {i//1000 + 1}/{total_chunks} 전송 중]")
                sender.send_data(df_chunk, url_vote, payload_vote)
        elif mode == 'local':
            url_vote = url_vote.replace('https://api.lawdigest.net', 'http://localhost:8080')
            print(f'[로컬 모드 : {url_vote}로 데이터 전송]')
            sender.send_data(df_vote, url_vote, payload_vote)
        elif mode == 'test':
            print('[테스트 모드 : 데이터 전송 생략]')
        elif mode == 'save':
            df_vote.to_csv('bills_vote.csv', index=False)
            print('[데이터 저장 완료]')

        if df_vote_party is None or df_vote_party.empty:
            print("❌ [ERROR] 정당별 표결 결과 데이터가 없습니다.")
            return df_vote, None

        payload_party = os.getenv('PAYLOAD_vote_party')
        url_party = os.getenv('POST_URL_vote_party')

        if mode == 'remote':
            total_chunks = len(df_vote_party) // 1000 + (1 if len(df_vote_party) % 1000 > 0 else 0)
            for i in range(0, len(df_vote_party), 1000):
                df_chunk = df_vote_party.iloc[i:i + 1000]
                print(f"[정당별 표결 청크 {i//1000 + 1}/{total_chunks} 전송 중]")
                sender.send_data(df_chunk, url_party, payload_party)
        elif mode == 'local':
            url_party = url_party.replace('https://api.lawdigest.net', 'http://localhost:8080')
            print(f'[로컬 모드 : {url_party}로 데이터 전송]')
            sender.send_data(df_vote_party, url_party, payload_party)
        elif mode == 'test':
            print('[테스트 모드 : 데이터 전송 생략]')
        elif mode == 'save':
            df_vote_party.to_csv('vote_party.csv', index=False)
            print('[데이터 저장 완료]')

        return df_vote, df_vote_party

    def update_bills_alternatives(self, start_ord=None, end_ord=None):
        """대안-법안 관계 데이터를 수집하고 모드에 따라 저장 또는 전송하는 메서드"""

        fetch_mode = 'total'

        if fetch_mode != 'total':
            print("현재는 'total' 모드만 지원합니다.")
            return None

        api_key = os.environ.get("APIKEY_billsContent")
        url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'

        params = {
            'serviceKey': api_key,
            'numOfRows': '100',
            'start_ord': start_ord or os.getenv('AGE'),
            'end_ord': end_ord or os.getenv('AGE'),
            'proposer_kind_cd': 'F02'
        }

        all_data = []
        pageNo = 1
        max_retry = 3
        start_time = time.time()

        while True:
            params.update({'pageNo': str(pageNo)})
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    root = ElementTree.fromstring(response.content)
                    items = root.find('body').find('items')
                    if items is None or len(items) == 0:
                        break
                    data = [{child.tag: child.text for child in item} for item in items]
                    all_data.extend(data)
                else:
                    print(f"❌ [ERROR] 응답 코드: {response.status_code} (Page {pageNo})")
                    max_retry -= 1
            except Exception as e:
                print(f"❌ [ERROR] 데이터 처리 중 오류 발생: {e}")
                max_retry -= 1

            if max_retry <= 0:
                print("🚨 [WARNING] 최대 재시도 횟수 초과! 데이터 수집 중단.")
                break

            pageNo += 1

        df_bills_content = pd.DataFrame(all_data)

        end_time = time.time()
        print(f"\n✅ [INFO] 모든 파일 다운로드 완료! ⏳ 전체 소요 시간: {end_time - start_time:.2f}초")
        print(f"📌 [INFO] 총 {len(df_bills_content)} 개의 법안 수집됨.")

        if df_bills_content.empty:
            print("❌ [ERROR] 수집한 데이터가 없습니다.")
            return None

        df_alt_ids = df_bills_content[['proposeDt', 'billId', 'proposerKind']]

        fetcher = DataFetcher(params=None)
        df_alternatives = fetcher.fetch_bills_alternatives(df_alt_ids)

        if df_alternatives is None or df_alternatives.empty:
            print("❌ [ERROR] 대안 데이터가 없습니다.")
            return None

        mode = self.mode

        payload_name = os.getenv('PAYLOAD_alternatives')
        url_post = os.getenv('POST_URL_alternatives')

        sender = APISender()

        if mode == 'remote' and url_post and payload_name:
            total_chunks = len(df_alternatives) // 1000 + (1 if len(df_alternatives) % 1000 > 0 else 0)
            for i in range(0, len(df_alternatives), 1000):
                df_chunk = df_alternatives.iloc[i:i + 1000]
                print(f"[대안 관계 청크 {i//1000 + 1}/{total_chunks} 전송 중]")
                sender.send_data(df_chunk, url_post, payload_name)
        elif mode == 'local' and url_post and payload_name:
            url_post = url_post.replace('https://api.lawdigest.net', 'http://localhost:8080')
            print(f'[로컬 모드 : {url_post}로 데이터 전송]')
            sender.send_data(df_alternatives, url_post, payload_name)
        elif mode == 'test':
            print('[테스트 모드 : 데이터 전송 생략]')
        elif mode == 'save':
            df_alternatives.to_csv('bills_alternatives.csv', index=False)
            print('[데이터 저장 완료]')

        return df_alternatives

    def update_commitee_info(self):
        pass
