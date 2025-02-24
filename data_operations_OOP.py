import requests
import pandas as pd
from xml.etree import ElementTree
import time
from datetime import datetime, timedelta
from IPython.display import clear_output 
from openai import OpenAI
import json
import os
import pymysql
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import sys

import pymysql
import os
from dotenv import load_dotenv

class DatabaseManager:
    """MySQL RDS 연결 및 데이터베이스 관련 기능"""

    def __init__(self, host=None, port=None, username=None, password=None, database=None):
        """
        DatabaseManager 클래스 초기화

        Args:
            host (str): 데이터베이스 서버 주소 (환경 변수: `host`)
            port (int): 데이터베이스 포트 (환경 변수: `port`)
            username (str): 데이터베이스 사용자명 (환경 변수: `username`)
            password (str): 데이터베이스 비밀번호 (환경 변수: `password`)
            database (str): 사용할 데이터베이스명 (환경 변수: `database`)
        """
        load_dotenv()  # .env 파일 로드 (있을 경우)

        self.host = host or os.environ.get("host")
        self.port = int(port or os.environ.get("port", 3306))  # 기본값 3306
        self.username = username or os.environ.get("username")
        self.password = password or os.environ.get("password")
        self.database = database or os.environ.get("database")

        self.connection = None
        self.connect()  # 클래스 생성 시 자동 연결

    def connect(self):
        """MySQL RDS 데이터베이스 연결"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            print(f"✅ [INFO] Database connected successfully: {self.host}:{self.port} (DB: {self.database})")
        except pymysql.MySQLError as e:
            print(f"❌ [ERROR] Database connection failed: {e}")
            self.connection = None

    def execute_query(self, query, params=None, fetch_one=False):
        """
        데이터베이스에서 SQL 쿼리를 실행하고 결과를 반환.

        Args:
            query (str): 실행할 SQL 쿼리문
            params (tuple, optional): SQL 쿼리의 파라미터
            fetch_one (bool): True이면 첫 번째 결과만 반환, False이면 전체 결과 반환

        Returns:
            list or dict: 쿼리 결과 데이터 (SELECT 문일 경우)
        """
        if not self.connection:
            print("❌ [ERROR] Database connection is not available.")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchone() if fetch_one else cursor.fetchall()
        except pymysql.MySQLError as e:
            print(f"❌ [ERROR] Query execution failed: {e}")
            return None

    def get_latest_propose_date(self):
        """RDS 데이터베이스에서 가장 최근의 법안 발의 날짜를 가져오는 함수"""
        try:
            query = "SELECT MAX(propose_date) AS latest_date FROM Bill"
            result = self.execute_query(query, fetch_one=True)
            return result["latest_date"] if result else None
        except Exception as e:
            print("❌ [ERROR] Failed to fetch the latest propose_date")
            print(e)
            return None

    def get_latest_timeline_date(self):
        """RDS 데이터베이스에서 가장 최근의 법안 처리 날짜를 가져오는 함수"""
        try:
            query = "SELECT MAX(status_update_date) AS latest_date FROM BillTimeline"
            result = self.execute_query(query, fetch_one=True)
            return result["latest_date"] if result else None
        except Exception as e:
            print("❌ [ERROR] Failed to fetch the latest status_update_date")
            print(e)
            return None

        def get_existing_bill_ids(self):
            """이미 존재하는 법안 ID 목록을 반환"""
            query = "SELECT bill_id FROM bills"
            results = self.execute_query(query)
            return [row["bill_id"] for row in results] if results else []

    def close(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            print("✅ [INFO] Database connection closed.")


class DataFetcher:
    def __init__(self, subject, params=None, url=None, filter_data=True):
        
        self.subject = str(subject) # 수집대상
        if params == None:
            self.params = {}
        else:
            self.params = params # 요청변수
        self.url = url # 모드(처리방식)
        self.filter_data = filter_data
        self.content = None # 수집된 데이터
        self.df_bills = None
        self.df_lawmakers = None

        load_dotenv()
        
        self.content = self.fetch_data()

    def fetch_data(self):
        match self.subject:
            case "bill_info":
                return self.fetch_bills_info()
            case "bill_content":
                return self.fetch_bills_content()
            case "bill_coactors":
                return self.fetch_bills_coactors()
            case "lawmakers":
                return self.fetch_lawmakers_data()
            case "bill_timeline":
                return self.fetch_bills_timeline()
            case "bill_result":
                return self.fetch_bills_result()
            case "bill_vote":
                return self.fetch_bills_vote()
            case "vote_party":
                return self.fetch_vote_party()
            case _:
                print(f"❌ [ERROR] '{self.subject}' is not a valid subject.")
                return None
        
    def fetch_bills_content(self):
        """
        법안 주요 내용 데이터를 API에서 수집하는 함수.
        """

        # 기본 날짜 설정
        start_date = self.params.get("start_date", (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        end_date = self.params.get("end_date", datetime.now().strftime('%Y-%m-%d'))

        # 환경 변수로부터 API 키 및 국회 회기 정보 로드
        api_key = os.environ.get("APIKEY_billsContent")
        url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'
        params = {
            'serviceKey': api_key,
            'numOfRows': '100',
            'start_ord': self.params.get("start_ord", os.environ.get("AGE")),
            'end_ord': self.params.get("end_ord", os.environ.get("AGE")),
            'start_propose_date': start_date,
            'end_propose_date': end_date
        }

        # 수집하는 날짜 범위 출력
        print(f"📌 [{start_date} ~ {end_date}] 의안 주요 내용 데이터 수집 시작...")

        # 데이터 수집 시작
        all_data = []
        page_no = 1
        processing_count = 0
        max_retry = 3
        start_time = time.time()

        while True:
            params.update({'pageNo': str(page_no)})
            response = requests.get(url, params=params)

            if response.status_code == 200:
                try:
                    root = ElementTree.fromstring(response.content)
                    items = root.find('body').find('items')
                    # XML 파싱

                    result_code = root.find('header/resultCode').text
                    result_msg = root.find('header/resultMsg').text


                    if not items:
                        print(f"✅ [INFO] 모든 페이지 데이터 수집 완료. 총 {len(all_data)} 개의 항목 수집됨.")
                        break

                    data = [{child.tag: child.text for child in item} for item in items]
                    all_data.extend(data)

                    processing_count += 1
                except ElementTree.ParseError:
                    print(f"❌ [ERROR] XML Parsing Error (Page {page_no}): {response.text}")
                    max_retry -= 1
                except Exception as e:
                    print(f"❌ [ERROR] Unexpected Error (Page {page_no}): {e}")
                    max_retry -= 1
            else:
                print(f"❌ [ERROR] HTTP Request Failed (Status Code: {response.status_code})")
                max_retry -= 1

            if max_retry <= 0:
                print("❌ [ERROR] Maximum retry limit reached. Exiting...")
                break

            page_no += 1

        # 결과 출력
        print(f"📌 [INFO] API 응답 코드: {result_code}, 메시지: {result_msg}")

        # 데이터프레임 생성
        df_bills_content = pd.DataFrame(all_data)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"✅ [INFO] 모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초")
        print(f"✅ [INFO] 총 {len(df_bills_content)} 개의 법안 수집됨.")


        # 수집한 데이터가 없으면 AssertionError 발생
        assert len(df_bills_content) > 0, "❌ [ERROR] 수집된 데이터가 없습니다. API 응답을 확인하세요."

        if self.filter_data:
            print("✅ [INFO] 데이터 컬럼 필터링을 수행합니다.")
            # 유지할 컬럼 목록
            columns_to_keep = [
                'proposeDt',  # 발의일자
                'billNo',  # 법안번호
                'summary',  # 주요내용
                'procStageCd',  # 현재 처리 단계
                'proposerKind'
            ]

            # 지정된 컬럼만 유지하고 나머지 제거
            df_bills_content = df_bills_content[columns_to_keep]

            # 'summary' 컬럼에 결측치가 있는 행 제거
            df_bills_content = df_bills_content.dropna(subset=['summary'])

            # 인덱스 재설정
            df_bills_content.reset_index(drop=True, inplace=True)

            # 컬럼 이름 변경
            df_bills_content.rename(columns={
                "proposeDt": "proposeDate",
                "billNo": "billNumber",
                "summary": "summary",
                "procStageCd": "stage"
            }, inplace=True)

            print(f"✅ [INFO] 결측치 처리 완료. {len(df_bills_content)} 개의 법안 유지됨.")
            print("\n📌 발의일자별 수집한 데이터 수:")
            print(df_bills_content['proposeDate'].value_counts()) 

        else:
            print("✅ [INFO] 데이터 컬럼 필터링을 수행하지 않습니다.")
            print("\n📌 발의일자별 수집한 데이터 수:")
            print(df_bills_content["proposeDt"].value_counts()) 

        

        self.content = df_bills_content
        self.df_bills = df_bills_content

        return df_bills_content

    def fetch_bills_info(self):
            """
            법안 기본 정보를 API에서 가져오는 함수.
            """

            # bill_id가 있는 법안 내용 데이터 수집
            if self.df_bills is None:
                print("✅ [INFO] 법안정보 수집 대상 bill_no 수집을 위해 법안 내용 API로부터 정보를 수집합니다.")
                df_bills = self.fetch_bills_content()
            else:
                df_bills = self.df_bills

            # 데이터프레임이 없으면 예외 처리
            if df_bills is None or df_bills.empty:
                print("❌ [ERROR] `df_bills` 데이터가 없습니다. 올바른 값을 전달하세요.")
                return None

            # API 정보 설정
            api_key = os.environ.get("APIKEY_billsInfo")
            url = self.url or "https://open.assembly.go.kr/portal/openapi/ALLBILL"
            all_data = []

            print(f"\n📌 [법안 정보 데이터 수집 중...]")
            start_time = time.time()

            # `df_bills`에서 법안 번호(`billNumber`) 가져오기
            for row in tqdm(df_bills.itertuples(), total=len(df_bills)):
                params = {
                    "Key": api_key,
                    "Type": "json",
                    "pSize": 5,
                    "pIndex": 1,
                    "BILL_NO": row.billNumber  # 법안 번호
                }

                try:
                    response = requests.get(url, params=params, timeout=10)
                    response.raise_for_status()

                    # JSON 데이터 파싱
                    response_data = response.json()
                    items = response_data.get("ALLBILL", [])

                    if len(items) > 1:
                        data = items[1].get('row', [])
                        if data:
                            all_data.extend(data)
                        else:
                            break
                    else:
                        break

                except requests.exceptions.RequestException as e:
                    print(f"❌ [ERROR] 요청 오류: {e}")
                    continue  # 오류 발생 시 다음 항목으로 이동
                except requests.exceptions.JSONDecodeError:
                    print(f"❌ [ERROR] JSON 파싱 오류: {response.text}")
                    continue
                except Exception as e:
                    print(f"❌ [ERROR] 예상치 못한 오류: {e}")
                    continue

            # DataFrame 생성
            df_bills_info = pd.DataFrame(all_data)

            end_time = time.time()
            total_time = end_time - start_time
            print(f"✅ [INFO] 다운로드 완료! 총 소요 시간: {total_time:.2f}초")

            # 데이터가 없으면 종료
            if df_bills_info.empty:
                print("❌ [ERROR] 수집한 데이터가 없습니다.")
                return None

            print(f"✅ [INFO] 총 {len(df_bills_info)}개의 법안 정보 데이터가 수집되었습니다.")

            if self.filter_data:
                print("✅ [INFO] 데이터 컬럼 필터링을 수행합니다.")
                # 컬럼 필터링
                columns_to_keep = ['ERACO', 'BILL_ID', 'BILL_NO', 'BILL_NM', 'PPSR_NM', 'JRCMIT_NM']
                df_bills_info = df_bills_info[columns_to_keep]

                # 컬럼명 변경
                column_mapping = {
                    'ERACO': 'assemblyNumber',
                    'BILL_ID': 'billId',
                    'BILL_NO': 'billNumber',
                    'BILL_NM': 'billName',
                    'PPSR_NM': 'proposers',
                    'JRCMIT_NM': 'committee'
                }
                df_bills_info.rename(columns=column_mapping, inplace=True)

                # 정규 표현식을 사용하여 이름을 추출하는 함수 정의
                def extract_names(proposer_str):
                    return re.findall(r'[가-힣]+(?=의원)', proposer_str) if isinstance(proposer_str, str) else []

                df_bills_info['rstProposerNameList'] = df_bills_info['proposers'].apply(extract_names)

                df_bills_info['assemblyNumber'] = df_bills_info['assemblyNumber'].str.replace(r'\D', '', regex=True)

                print("✅ [INFO] 컬럼 필터링 및 컬럼명 변경 완료.")

            self.content = df_bills_info

            return df_bills_info

    def fetch_lawmakers_data(self):
        """
        국회의원 데이터를 API로부터 가져와서 DataFrame으로 반환하는 함수.
        API 키와 URL은 함수 내부에서 정의되며, 모든 페이지를 처리합니다.

        Returns:
        - df_lawmakers: pandas.DataFrame, 수집된 국회의원 데이터
        """
        api_key = os.environ.get("APIKEY_lawmakers")
        url = 'https://open.assembly.go.kr/portal/openapi/nwvrqwxyaytdsfvhu'
        p_size = 100
        max_retry = 10

        all_data = []
        pageNo = 1
        processing_count = 0
        retries_left = max_retry

        start_time = time.time()

        while True:
            params = {
                'KEY': api_key,
                'Type': 'xml',
                'pIndex': pageNo,
                'pSize': p_size,
            }
            
            print(f"Requesting page {pageNo}...")
            
            # API 요청
            response = requests.get(url, params=params)
            
            # 응답 데이터 확인
            if response.status_code == 200:
                try:
                    root = ElementTree.fromstring(response.content)
                    head = root.find('head')
                    if head is None:
                        print(f"Error: 'head' element not found in response (Page {pageNo})")
                        break
                    
                    total_count_elem = head.find('list_total_count')
                    if total_count_elem is None:
                        print(f"Error: 'list_total_count' element not found in 'head' (Page {pageNo})")
                        break
                    
                    total_count = int(total_count_elem.text)
                    
                    rows = root.findall('row')
                    if not rows:
                        print("No more data available.")
                        break
                    
                    data = []
                    for row_elem in rows:
                        row = {child.tag: child.text for child in row_elem}
                        data.append(row)
                    
                    all_data.extend(data)
                    # print(f"Page {pageNo} processed. {len(data)} items added. Total: {len(all_data)}")
                    processing_count += 1
                    
                    if pageNo * p_size >= total_count:
                        # print("All pages processed.")
                        break
                    
                except Exception as e:
                    print(f"Error: {e}")
                    retries_left -= 1
            else:
                print(f"Error Code: {response.status_code} (Page {pageNo})")
                retries_left -= 1
            
            if retries_left <= 0:
                print("Maximum retry reached. Exiting...")
                break
            
            if processing_count >= 10:
                clear_output()
                processing_count = 0

            pageNo += 1

        # 데이터프레임 생성
        df_lawmakers = pd.DataFrame(all_data)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
        print(f"[{len(df_lawmakers)} 개의 의원 데이터 수집됨]")

        self.content = df_lawmakers

        return df_lawmakers


    def fetch_bills_coactors(self):
            """
            billId를 사용하여 각 법안의 공동 발의자 명단을 수집하는 함수.
            """

            # `df_bills`가 없으면 `fetch_bills_content()`를 호출하여 자동으로 수집
            if self.df_bills is None:
                print("✅ [INFO] 법안 공동발의자 명단 정보 수집 대상 bill_no 수집을 위해 법안 내용 API로부터 정보를 수집합니다.")
                self.df_bills = self.fetch_bills_info()

            # 데이터가 없으면 종료
            if self.df_bills is None or self.df_bills.empty:
                print("❌ [ERROR] 법안 데이터가 없습니다.")
                return None

            coactors_data = []
            
            # 국회의원 데이터 가져오기
            df_lawmakers = self.fetch_lawmakers_data()

            print(f"📌 [INFO] 공동 발의자 정보 수집 시작... 총 {len(self.df_bills)} 개의 법안 대상")
            
            # 각 법안의 billId에 대해 공동 발의자 정보를 수집
            for billId in tqdm(self.df_bills['billId']):
                url = f"http://likms.assembly.go.kr/bill/coactorListPopup.do?billId={billId}"
                
                # HTML 가져오기
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                except requests.RequestException as e:
                    print(f"❌ [ERROR] Failed to fetch data for billId {billId}: {e}")
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                coactors_section = soup.find('div', {'class': 'links textType02 mt20'})

                if coactors_section is None:
                    print(f"❌ [ERROR] 공동 발의자 명단을 찾을 수 없습니다 for billId {billId}.")
                    continue
                
                # 공동 발의자 정보 추출
                for a_tag in coactors_section.find_all('a'):
                    coactor_text = a_tag.get_text(strip=True)
                    match = re.match(r'(.+?)\((.+?)/(.+?)\)', coactor_text)
                    if match:
                        proposer_name, proposer_party, proposer_hj_name = match.groups()
                        coactors_data.append([billId, proposer_name, proposer_party, proposer_hj_name])

            # DataFrame 생성
            df_coactors = pd.DataFrame(coactors_data, columns=['billId', 'ProposerName', 'ProposerParty', 'ProposerHJName'])

            # 공동 발의자 ID 매칭
            proposer_codes = []
            for _, row in df_coactors.iterrows():
                match = df_lawmakers[
                    (df_lawmakers['HG_NM'] == row['ProposerName']) &
                    (df_lawmakers['POLY_NM'] == row['ProposerParty']) &
                    (df_lawmakers['HJ_NM'] == row['ProposerHJName'])
                ]
                proposer_codes.append(match['MONA_CD'].values[0] if not match.empty else None)

            df_coactors['publicProposerIdList'] = proposer_codes

            # billId 기준으로 리스트 형태로 그룹화
            df_coactors = df_coactors.groupby('billId').agg({
                'publicProposerIdList': lambda x: x.dropna().tolist(),
                'ProposerName': lambda x: x.dropna().tolist()
            }).reset_index()

            print(f"✅ [INFO] 공동 발의자 정보 수집 완료. 총 {len(df_coactors)} 개의 법안 대상")

            return df_coactors

    def fetch_bills_timeline(self):
        all_data = []
        pageNo = 1
        processing_count = 0
        start_time = time.time()

        start_date_str = self.params.get("start_date") or (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        end_date_str = self.params.get("end_date") or datetime.now().strftime('%Y-%m-%d')
        age = self.params.get("age") or os.environ.get("AGE")

        # 문자열을 datetime 객체로 변환
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        date_range = (end_date - start_date).days + 1

        print(f"\n[{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 의정활동 데이터 수집]")

        max_retry = 3

        url="https://open.assembly.go.kr/portal/openapi/nqfvrbsdafrmuzixe"

        for single_date in (start_date + timedelta(n) for n in range(date_range)):
            date_str = single_date.strftime('%Y-%m-%d')

            while True:
                params = {
                    "Key": os.environ.get("APIKEY_status"),
                    "Type": "xml",
                    "pIndex": pageNo,
                    "pSize": 100,
                    "AGE": age,
                    "DT": date_str
                }

                try:
                    response = requests.get(url, params=params, timeout=10)

                    if response.status_code == 200:
                        root = ElementTree.fromstring(response.content)
                        items = root.findall(".//row")

                        if not items:
                            break  # 더 이상 데이터 없음

                        data = [{child.tag: child.text for child in item} for item in items]
                        all_data.extend(data)
                        print(f"Data for {date_str}, page {pageNo} processed. {len(data)} items added. total: {len(all_data)}")
                        processing_count += 1
                    else:
                        print(f"Error Code: {response.status_code} (Date: {date_str}, Page {pageNo})")
                        max_retry -= 1

                except Exception as e:
                    print(f"Error processing response: {str(e)}")
                    max_retry -= 1

                if max_retry <= 0:
                    print("Maximum retry reached. Exiting...")
                    break

                if processing_count >= 10:
                    processing_count = 0

                pageNo += 1

            pageNo = 1  # 날짜가 변경되면 페이지 번호 초기화

        df_timeline = pd.DataFrame(all_data)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초")
        print(f"{len(df_timeline)} 개의 의정활동 데이터가 수집됨.")

        self.content = df_timeline

        return df_timeline