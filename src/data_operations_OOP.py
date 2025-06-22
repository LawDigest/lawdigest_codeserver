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
    
    def get_existing_bill_ids(self, bill_ids):
            """데이터베이스에 이미 존재하는 법안 id를 반환하는 함수"""

            format_strings = ','.join(['%s'] * len(bill_ids)) 

            # **수정된 부분:** query를 문자열로만 만들고, bill_ids 튜플을 params로 전달
            query = f"SELECT {'bill_id'} FROM Bill WHERE {'bill_id'} IN ({format_strings})"
            params = tuple(bill_ids) # 매개변수를 별도의 변수로 분리

            result = self.execute_query(query, params=params, fetch_one=False) # params 인자를 명시적으로 전달
            # print(result)

            # Extract IDs from the result
            existing_ids = [row['bill_id'] for row in result]

            print(f"DB에 존재하는 법안 id 목록: {existing_ids}")

            return existing_ids

    def close(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            print("✅ [INFO] Database connection closed.")


class DataFetcher:
    def __init__(self, params, subject=None, url=None, filter_data=True):
        if params == None:
            self.params = {}
        else:
            self.params = params # 요청변수
        self.url = url # 모드(처리방식)
        self.filter_data = filter_data
        self.content = None # 수집된 데이터
        self.df_bills = None
        self.df_lawmakers = None
        self.df_vote = None
        self.subject = subject

        load_dotenv()
        
        self.content = self.fetch_data(self.subject)

    def fetch_data(self, subject):
        match subject:
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
            case "alternative_bill":
                return self.fetch_bills_alternatives()
            case None:
                return None
            case _:
                print(f"❌ [ERROR] '{subject}' is not a valid subject.")
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

            

            print(f"✅ [INFO] 결측치 처리 완료. {len(df_bills_content)} 개의 법안 유지됨.")

        else:
            print("✅ [INFO] 데이터 컬럼 필터링을 수행하지 않습니다.")

        # 컬럼 이름 변경
        df_bills_content.rename(columns={
            "proposeDt": "proposeDate",
            "billNo": "billNumber",
            "summary": "summary",
            "procStageCd": "stage"
        }, inplace=True)

        print("\n📌 발의일자별 수집한 데이터 수:")
        print(df_bills_content['proposeDate'].value_counts()) 

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
                            continue
                    else:
                        continue

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


    def fetch_bills_coactors(self, df_bills=None):
            """
            billId를 사용하여 각 법안의 공동 발의자 명단을 수집하는 함수.
            """

            # `df_bills`가 없으면 `fetch_bills_content()`를 호출하여 자동으로 수집
            if df_bills is None:
                print("✅ [INFO] 법안 공동발의자 명단 정보 수집 대상 bill_no 수집을 위해 법안 내용 API로부터 정보를 수집합니다.")
                df_bills = self.fetch_bills_info()

            # 데이터가 없으면 종료
            if df_bills is None or df_bills.empty:
                print("❌ [ERROR] 법안 데이터가 없습니다.")
                return None

            coactors_data = []
            
            # 국회의원 데이터 가져오기
            df_lawmakers = self.fetch_lawmakers_data()

            print(f"📌 [INFO] 공동 발의자 정보 수집 시작... 총 {len(df_bills)} 개의 법안 대상")
            
            # 각 법안의 billId에 대해 공동 발의자 정보를 수집
            for billId in tqdm(df_bills['billId']):
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

        print(f"\n📌 [INFO] [{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}] 의정활동 데이터 수집 시작...")

        max_retry = 3

        url = "https://open.assembly.go.kr/portal/openapi/nqfvrbsdafrmuzixe"

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
                        print(f"✅ [INFO] {date_str} | 📄 Page {pageNo} | 📊 {len(data)} 개 추가됨. 총 {len(all_data)} 개 수집됨.")
                        processing_count += 1
                    else:
                        print(f"❌ [ERROR] 응답 코드: {response.status_code} (📅 Date: {date_str}, 📄 Page {pageNo})")
                        max_retry -= 1

                except Exception as e:
                    print(f"❌ [ERROR] 응답 처리 중 오류 발생: {str(e)}")
                    max_retry -= 1

                if max_retry <= 0:
                    print("🚨 [WARNING] 최대 재시도 횟수 초과! 데이터 수집 중단.")
                    break

                if processing_count >= 10:
                    processing_count = 0

                pageNo += 1

            pageNo = 1  # 날짜가 변경되면 페이지 번호 초기화

        df_timeline = pd.DataFrame(all_data)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n✅ [INFO] 모든 파일 다운로드 완료! ⏳ 전체 소요 시간: {total_time:.2f}초")
        print(f"📌 [INFO] 총 {len(df_timeline)} 개의 의정활동 데이터 수집됨.")

        self.content = df_timeline

        return df_timeline


    def fetch_bills_result(self):
        # start_date와 end_date를 self.params에서 가져오거나 기본값(오늘 날짜)으로 설정
        start_date = datetime.strptime(self.params.get("start_date", datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')
        end_date = datetime.strptime(self.params.get("end_date", datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')
        
        # 나이(age) 파라미터 설정
        age = self.params.get("age") or os.getenv("AGE")
        
        api_key = os.getenv("APIKEY_result")
        url = 'https://open.assembly.go.kr/portal/openapi/TVBPMBILL11'
        
        all_data = []
        processing_count = 0
        max_retry = 10
        
        print(f"\n📌 [INFO] [{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}] 법안 결과 데이터 수집 시작...")
        start_time = time.time()
        
        current_date = start_date
        while current_date <= end_date:
            pageNo = 1
            while True:
                params = {
                    'KEY': api_key,
                    'Type': 'xml',
                    'pIndex': pageNo,
                    'pSize': 100,
                    'AGE': age,
                    'PROC_DT': current_date.strftime('%Y-%m-%d')
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    try:
                        root = ElementTree.fromstring(response.content)
                        head = root.find('head')
                        if head is None:
                            break
                        total_count_elem = head.find('list_total_count')
                        if total_count_elem is None:
                            break
                        total_count = int(total_count_elem.text)
                        
                        rows = root.findall('row')
                        if not rows:
                            break
                        
                        data = [{child.tag: child.text for child in row_elem} for row_elem in rows]
                        all_data.extend(data)
                        print(f"✅ [INFO] {current_date.strftime('%Y-%m-%d')} | 📄 Page {pageNo} | 📊 Total: {len(all_data)} 개 수집됨.")
                        processing_count += 1
                        
                        if pageNo * 100 >= total_count:
                            break
                    except Exception as e:
                        print(f"❌ [ERROR] 데이터 처리 중 오류 발생: {e}")
                        max_retry -= 1
                else:
                    print(f"❌ [ERROR] 응답 코드: {response.status_code} (📄 Page {pageNo})")
                    max_retry -= 1
                
                if max_retry <= 0:
                    print("🚨 [WARNING] 최대 재시도 횟수 초과! 데이터 수집 중단.")
                    break
                
                pageNo += 1
            current_date += timedelta(days=1)
        
        df_result = pd.DataFrame(all_data)
        
        if df_result.empty:
            print("⚠️ [WARNING] 수집된 데이터가 없습니다.")
            self.content = None
            return None
        
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n✅ [INFO] 모든 파일 다운로드 완료! ⏳ 전체 소요 시간: {total_time:.2f}초")
        print(f"📌 [INFO] 총 {len(df_result)} 개의 법안 수집됨.")
        
        pd.set_option('display.max_columns', None)
        
        self.content = df_result
        return df_result

    def fetch_bills_vote(self):
        # 환경 변수 로드
        api_key = os.getenv("APIKEY_status")

        # start_date, end_date 기본값 설정 (어제 ~ 오늘)
        start_date_str = self.params.get("start_date", (datetime.now() - timedelta(1)).strftime('%Y-%m-%d'))
        end_date_str = self.params.get("end_date", datetime.now().strftime('%Y-%m-%d'))
        age = self.params.get("age") or os.getenv("AGE")

        # 문자열을 datetime 객체로 변환
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        date_range = (end_date - start_date).days + 1

        url = 'https://open.assembly.go.kr/portal/openapi/nwbpacrgavhjryiph'
        all_data = []
        pageNo = 1
        processing_count = 0
        start_time = time.time()

        print(f"\n📌 [INFO] [{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}] 본회의 의결 데이터 수집 시작...")

        for single_date in (start_date + timedelta(n) for n in range(date_range)):
            date_str = single_date.strftime('%Y-%m-%d')

            while True:
                params = {
                    'KEY': api_key,
                    'Type': 'xml',
                    'pIndex': pageNo,
                    'pSize': 100,
                    'AGE': age,
                    'RGS_PROC_DT': date_str  # 본회의심의_의결일 필터링
                }

                try:
                    response = requests.get(url, params=params, timeout=10)

                    if response.status_code == 200:
                        root = ElementTree.fromstring(response.content)
                        head = root.find('head')
                        if head is None:
                            break

                        total_count_elem = head.find('list_total_count')
                        if total_count_elem is None:
                            break

                        total_count = int(total_count_elem.text)
                        rows = root.findall('row')

                        if not rows:
                            print(f"⚠️ [WARNING] {date_str} 데이터 없음. (📄 Page {pageNo})")
                            break

                        data = [{child.tag: child.text for child in row_elem} for row_elem in rows]
                        all_data.extend(data)
                        print(f"✅ [INFO] {date_str} | 📄 Page {pageNo} | 📊 Total: {len(all_data)} 개 수집됨.")
                        processing_count += 1

                        if pageNo * 100 >= total_count:
                            break

                    else:
                        print(f"❌ [ERROR] 응답 코드: {response.status_code} (📄 Page {pageNo})")
                        break

                except Exception as e:
                    print(f"❌ [ERROR] 데이터 처리 중 오류 발생: {e}")
                    break

                pageNo += 1

            pageNo = 1  # 다음 날짜로 넘어갈 때 페이지 번호 초기화

        # 데이터프레임 생성
        df_vote = pd.DataFrame(all_data)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n✅ [INFO] 모든 파일 다운로드 완료! ⏳ 전체 소요 시간: {total_time:.2f}초")
        print(f"📌 [INFO] 총 {len(df_vote)} 개의 본회의 의결 데이터 수집됨.")

        self.df_vote = df_vote
        self.content = df_vote

        return df_vote

    def fetch_vote_party(self):
        # 환경 변수 로드
        api_key = os.getenv("APIKEY_status")
        age = self.params.get("age") or os.getenv("AGE")
        url = 'https://open.assembly.go.kr/portal/openapi/nojepdqqaweusdfbi'

        all_data = []
        count = 0
        pageNo = 1
        processing_count = 0
        max_retry = 10

        start_time = time.time()

        df_vote = self.df_vote
        if df_vote == None:
            print("⚠️ [WARNING] 수집에 필요한 df_vote 데이터가 없습니다.")
            self.fetch_bills_vote()
            if len(self.df_vote) == 0:
                print("🚨 [WARNING] 해당 날짜에 수집 가능한 데이터가 없습니다. 코드를 종료합니다.")
                return None
                
        df_vote = self.df_vote

        print(f"\n📌 [INFO] 법안별 정당별 투표 결과 데이터 수집 시작...")

        for bill_id in df_vote[df_vote['PROC_RESULT_CD'] != '철회']['BILL_ID']:
            pageNo = 1
            while True:
                print(f"🔍 [INFO] 법안 ID: {bill_id} 처리 중...")
                params = {
                    'KEY': api_key,
                    'Type': 'xml',
                    'pIndex': pageNo,
                    'pSize': 100,
                    'AGE': age,
                    'BILL_ID': bill_id
                }

                count += 1
                print(f"📄 [INFO] 페이지 {pageNo} 요청 중...")

                try:
                    response = requests.get(url, params=params, timeout=10)

                    if response.status_code == 200:
                        root = ElementTree.fromstring(response.content)
                        head = root.find('head')
                        if head is None:
                            print(f"⚠️ [WARNING] 응답에 'head' 요소가 없습니다. (📄 Page {pageNo})")
                            break

                        total_count_elem = head.find('list_total_count')
                        if total_count_elem is None:
                            print(f"⚠️ [WARNING] 'list_total_count' 요소가 없습니다. (📄 Page {pageNo})")
                            break

                        total_count = int(total_count_elem.text)
                        rows = root.findall('row')

                        if not rows:
                            print(f"⚠️ [WARNING] {bill_id}에 대한 추가 데이터 없음. (📄 Page {pageNo})")
                            break

                        data = [{child.tag: child.text for child in row_elem} for row_elem in rows]
                        all_data.extend(data)
                        print(f"✅ [INFO] 📄 Page {pageNo} | 📊 총 {len(all_data)} 개 데이터 수집됨.")

                        processing_count += 1

                        if pageNo * 100 >= total_count:
                            print(f"✅ [INFO] 법안 ID: {bill_id}의 모든 페이지 처리 완료.")
                            break

                    else:
                        print(f"❌ [ERROR] 응답 코드: {response.status_code} (📄 Page {pageNo})")
                        break

                except Exception as e:
                    print(f"❌ [ERROR] 데이터 처리 중 오류 발생: {e}")
                    break

                if max_retry <= 0:
                    print("🚨 [WARNING] 최대 재시도 횟수 초과! 데이터 수집 중단.")
                    break

                pageNo += 1

        # 데이터프레임 생성
        df_vote_individual = pd.DataFrame(all_data)

        if df_vote_individual.empty:
            print("⚠️ [WARNING] 수집된 데이터가 없습니다.")
            self.content = None
            return None

        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n✅ [INFO] 모든 파일 다운로드 완료! ⏳ 전체 소요 시간: {total_time:.2f}초")
        print(f"📌 [INFO] 총 {len(df_vote_individual)} 개의 투표 데이터 수집됨.")

        # 필요한 컬럼만 유지
        columns_to_keep = [
            'AGE',  # 대수
            'BILL_ID',  # 의안번호
            'HG_NM',  # 의원명
            'POLY_NM',  # 소속정당
            'RESULT_VOTE_MOD',  # 표결결과
        ]
        df_vote_individual = df_vote_individual[columns_to_keep]

        # 정당별 찬성 투표 개수 집계
        df_vote_party = df_vote_individual[df_vote_individual['RESULT_VOTE_MOD'] == '찬성'] \
            .groupby(['BILL_ID', 'POLY_NM']) \
            .size() \
            .reset_index(name='voteForCount')

        # 컬럼 이름 변경
        df_vote_party.rename(columns={
            'BILL_ID': 'billId',
            'POLY_NM': 'partyName',
            'voteForCount': 'voteForCount'
        }, inplace=True)

        self.content = df_vote_party
        return df_vote_party

    def fetch_bills_alternatives(self, df_bills):
        """
        df_bills를 기반으로 각 법안의 대안을 수집하고 반환하는 메서드.

        Returns:
        pd.DataFrame: 각 법안의 대안을 포함하는 데이터프레임
        """

        # df_bills 확인 및 자동 수집
        if df_bills is None or df_bills.empty:
            print("⚠️ [WARNING] 수집된 법안 데이터(self.df_bills)가 없습니다. 법안 내용을 먼저 수집합니다...")
            df_bills = self.fetch_bills_info()

            # 수집 후에도 df_bills가 없으면 종료
            if df_bills is None or df_bills.empty:
                print("🚨 [WARNING] 법안 내용 데이터를 수집할 수 없습니다. 작업을 중단합니다.")
                return None

        def fetch_alternativeBills_relation_data(bill_id):
            """ 주어진 bill_id에 대한 대안 법안 데이터를 API에서 수집하는 내부 함수 """
            url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillAdditionalInfo'
            params = {
                'serviceKey': 'UJY+e286zOQsAHMHd/5cggpYFaFqG5mWawJKgrubJeKRBqVp0VUsyeHIgw/VGPQjWRSp6yaR/sUhXlhpKyv1cg==',
                'bill_id': bill_id
            }

            try:
                response = requests.get(url, params=params, timeout=10)

                if response.status_code == 200:
                    root = ElementTree.fromstring(response.content)
                    items = root.find('.//exhaust')

                    if items is None or len(items.findall('item')) == 0:
                        return []

                    law_data = []
                    for item in items.findall('item'):
                        bill_link = item.find('billLink').text
                        law_bill_id = bill_link.split('bill_id=')[-1]
                        bill_name = item.find('billName').text.encode('utf-8').decode('utf-8')  # 한글 디코딩
                        law_data.append({'billId': law_bill_id, 'billName': bill_name})

                    return law_data
                else:
                    print(f"❌ [ERROR] API 요청 실패 (bill_id={bill_id}), 응답 코드: {response.status_code}")
                    return []
            except Exception as e:
                print(f"❌ [ERROR] bill_id={bill_id} 처리 중 오류 발생: {e}")
                return []

        # 대안 데이터프레임 초기화
        alternatives_data = []

        print("📌 [INFO] 법안별 대안 데이터 수집 시작...")

        # tqdm을 사용하여 진행 상황 표시
        for _, row in tqdm(df_bills.iterrows(), total=len(df_bills)):
            alt_id = row['billId']  # 대안(위원장안) ID

            # 대안 데이터 수집
            law_data = fetch_alternativeBills_relation_data(alt_id)

            # 수집된 데이터를 리스트에 추가
            for law in law_data:
                alternatives_data.append({
                    'altBillId': alt_id,  # 대안(위원장안) ID
                    'billId': law['billId'],  # 대안에 포함된 법안 ID
                })

        # 대안 데이터를 데이터프레임으로 변환
        df_alternatives = pd.DataFrame(alternatives_data)

        if df_alternatives.empty:
            print("⚠️ [WARNING] 대안 법안 데이터를 수집하지 못했습니다.")
        else:
            print(f"✅ [INFO] 총 {len(df_alternatives)} 개의 대안 법안 데이터 수집 완료.")

        self.content = df_alternatives  # 클래스 속성에 저장
        return df_alternatives

class DataProcessor:

    def __init__(self, fetcher):
        self.fetcher = fetcher
    
    def process_congressman_bills(self, df_bills):
        """의원 발의 법안을 처리하는 함수
        
        Args:
            df_bills (pd.DataFrame) : 수집한 법안 데이터
            fetcher (DataFetcher) : 데이터 수집용 객체 - 공동발의자 데이터 수집 위해 필요
        Return:
            df_bills_congressman (pd.DataFrame) : 처리된 의원 발의 법안 데이터
        """
        df_bills_congressman = df_bills[df_bills['proposerKind'] == '의원'].copy()

        print(f"[의원 발의 법안 개수: {len(df_bills_congressman)}]")
        
        if(len(df_bills_congressman) == 0):
            print("[의원 발의 법안이 없습니다. 코드를 종료합니다.]")
            return pd.DataFrame()
        
         # df_bills_congressman에 발의자 정보 컬럼 머지
        print("\n[의원 발의자 데이터 병합 중...]")

        df_coactors = self.fetcher.fetch_bills_coactors()

        df_bills_congressman = pd.merge(df_bills_congressman, df_coactors, on='billId', how='inner')

        print("[의원 발의자 데이터 병합 완료]")

        def get_proposer_codes(row):
            name_list_length = len(row['rstProposerNameList'])
            return row['publicProposerIdList'][:name_list_length]

        # 새로운 컬럼 rstProposerIdList에 publicProposerIdList 리스트에서 슬라이싱한 값 추가
        print(df_bills_congressman.info())
        df_bills_congressman['rstProposerIdList'] = df_bills_congressman.apply(get_proposer_codes, axis=1)

        print(f"[의원 발의 법안 개수: {len(df_bills_congressman)}]")

        # 제외할 컬럼 목록
        # TODO: 이 부분 로직 과연 필요한 것인지 검토하고 불필요하면 삭제
        columns_to_drop = ['rstProposerNameList', 'ProposerName']
        df_bills_congressman.drop(columns=columns_to_drop, inplace=True)

        return df_bills_congressman

    def process_chairman_bills(self, df_bills):
        """ 위원장 발의 법안을 처리하는 함수
        
        Args:
            df_bills (pd.DataFrame) : 수집한 법안 데이터
            fetcher (DataFetcher) : 데이터 수집용 객체 - 대안-법안 관계 수집 위해 필요

        Returns:
            df_bills_chairman (pd.DataFrame) : 처리된 위원장 발의 법안 데이터
            df_alternatives (pd.DataFrame) : 위원장 발의 법안 데이터에 대한 대안-법안 포함관계 데이터
        """

        df_bills_chair = df_bills[df_bills['proposerKind'] == '위원장'].copy()

        if(len(df_bills_chair) == 0):
            print("[위원장 발의 법안이 없습니다. 코드를 종료합니다.]")
            return pd.DataFrame()

        # 위원장안 - 포함된 의원 관계 데이터 수집
        # TODO: df_alternatives 데이터 필요 - 어떻게 Fetch 해올지 고민
        df_alternatives = self.fetcher.fetch_bills_alternatives(df_bills)

        # df_bills_chair의 billName에서 (대안) 제거
        df_bills_chair['billName'] = df_bills_chair['billName'].str.replace(r'\(대안\)', '', regex=True)

        return df_bills_chair, df_alternatives

    def process_gov_bills(self, df_bills):
        """ 정부 발의 법안을 처리하는 함수

        Args: 
            df_bills (pd.DataFrame) : 수집한 법안 데이터
        
        Returns:
            df_bills_gov (pd.DataFrame) : 처리된 정부 발의 법안 데이터
        """
    
        df_bills_gov = df_bills[df_bills['proposerKind'] == '정부'].copy()

        if(len(df_bills_gov) == 0):
            print("[정부 발의 법안이 없습니다. 코드를 종료합니다.]")
            return pd.DataFrame()

        return df_bills_gov

    def merge_bills_df(self, df_bills_content, df_bills_info):
        print("\n[데이터프레임 병합 진행 중...]")
        # 'billNumber' 컬럼을 기준으로 두 데이터프레임을 병합
        df_bills = pd.merge(df_bills_content, df_bills_info, on='billNumber', how='inner')

        # BILL_NO가 중복되는 행 제거
        # df_bills = df_bills.drop_duplicates(subset='BILL_NO', keep='first')
        
        #billNumber가 중복되는 행 중 proposers가 '대통령'인 행 제거 => 대통령 거부권 행사한 법안 중복 제거
        df_bills = df_bills[~((df_bills['proposers'] == '대통령') & (df_bills['billNumber'].duplicated()))]

        # BILL_ID 결측치가 있는 행 제거
        df_bills = df_bills.dropna(subset=['billId'])

        #인덱스 재설정
        df_bills.reset_index(drop=True, inplace=True)
        
        print("데이터프레임 병합 완료")
        print(f"{len(df_bills)} 개의 법안 데이터 병합됨.")
        
        print(df_bills['proposeDate'].value_counts())
        
        return df_bills

    def add_AI_summary_columns(self, df_bills):
        df_bills['briefSummary'] = None
        df_bills['gptSummary'] = None
        print("\n[AI 요약 데이터 컬럼 추가 완료]")

        return df_bills

    def remove_duplicates(self, df_bills, DBManager):
        print("\n[DB와의 중복 데이터 제거 중...]")

        # 법안 ID 리스트 추출
        bill_ids = df_bills['billId'].tolist()

        # 기존 법안 ID 조회
        existing_ids = DBManager.get_existing_bill_ids(bill_ids)

        # 데이터프레임에서 DB에 존재하지 않는 법안 ID만 남기기
        df_bills_dup_removed = df_bills[~df_bills['billId'].isin(existing_ids)]
        
        # 중복 처리 결과 출력
        print(f"[총 {len(df_bills)}개의 법안 데이터 중 {len(df_bills_dup_removed)}개의 새로운 법안 데이터 발견됨.]")

        print(df_bills_dup_removed['proposeDate'].value_counts())

        return df_bills_dup_removed
    
    # TODO: 컬럼 필터링 수행하는 함수 DataProcessor에 구현
    def columns_selection(self):
        pass

# TODO: AISummarizer 구현
class AISummarizer:

    def __init__(self):
        self.input_data = None
        self.output_data = None
        self.proposer_type_list = ['congressman', 'chairman', 'gov'] # TODO: 자동 발의자 구분 인식 구현 이후 제거
        self.prompt_dict = {
            '의원':  "너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"{proposer}이 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1.핵심 내용: 설명 2.핵심 내용: 설명 3.핵심 내용: 설명 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해. 핵심 내용은 볼드체 처리해.",
            '위원장': "너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"{proposer}이 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1.핵심 내용: 설명 2.핵심 내용: 설명 3.핵심 내용: 설명 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해. 핵심 내용은 볼드체 처리해.",
            '정부':  "너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"대한민국 {proposer}가 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1.핵심 내용: 설명 2.핵심 내용: 설명 3.핵심 내용: 설명 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해. 핵심 내용은 볼드체 처리해."
        }

        # OpenAI Client 로드
        self.client = OpenAI(
        api_key=os.environ.get("APIKEY_OPENAI"),  # this is also the default, it can be omitted
        )

        # 환경변수 로드
        load_dotenv()

    def AI_title_summarize(self, df_bills, model=None):
    
        client = self.client
        
        if model is None:
            model = os.environ.get("TITLE_SUMMARIZATION_MODEL")    
        
        print("\n[AI 제목 요약 진행 중...]")
        
        # 'briefSummary' 컬럼이 공백이 아닌 경우에만 요약문을 추출하여 해당 컬럼에 저장
        total = df_bills['briefSummary'].isnull().sum()
        count = 0
        show_count = 0

        for index, row in df_bills.iterrows():
            count += 1
            print(f"현재 진행률: {count}/{total} | {round(count/total*100, 2)}%")

            content, title, id, proposer = row['summary'], row['billName'], row['billNumber'], row['proposers']
            print('-'*10)
            if not pd.isna(row['briefSummary']):
                print(f"{title}에 대한 요약문이 이미 존재합니다.")
                # clear_output()
                continue  
                # 이미 'SUMMARY', 'GPT_SUMMARY' 컬럼에 내용이 있으면 건너뜁니다

            task = f"\n위 내용의 핵심을 40글자 이내로 짧게 요약한 제목을 작성할 것. 제목은 반드시 {title}으로 끝나야 함."
            print(f"task: {task}")
            print('-'*10)

            messages = [
                {"role": "system",
                "content": "입력하는 법률개정안 내용의 핵심을 40글자 이내로 짧게 요약한 제목을 한 문장으로 작성할 것. 제목은 반드시 법률개정안 이름으로 끝나야 함.\n\n법률개정안의 내용을 한눈에 알아볼 수 있게 핵심을 요약한 제목을 작성. 반드시 '~하기 위한 ~법안'와 같은 형식으로 작성. 반드시 한 문장으로 작성. 법안의 취지를 중심으로 짧고 간결하게 요약\n"},          
                {"role": "user", "content": str(content) + str(task)}
            ]
            
            response = client.chat.completions.create(
                model=model,  
                messages=messages,
            )
            chat_response = response.choices[0].message.content

            print(f"chatGPT: {chat_response}")

            # 추출된 요약문을 'briefSummary' 컬럼에 저장
            df_bills.loc[df_bills['billNumber'] == id, 'briefSummary'] = chat_response
            show_count += 1

            if show_count % 5 == 0:
                clear_output()
        
        print(f"[법안 {count}건 요약 완료됨]")

        clear_output()
        
        print("[AI 제목 요약 완료]")

        self.output_data = df_bills

        return df_bills

    def AI_content_summarize(self, df_bills, model=None):
        """
        df_bills를 입력받아 'proposerKind' 컬럼을 기준으로 발의주체별 프롬프트를 자동으로 적용하여 AI 요약을 생성합니다.
        """
        client = self.client

        if model is None:
            model = os.environ.get("CONTENT_SUMMARIZATION_MODEL")

        print("\n[AI 내용 요약 진행 중...]")

        rows_to_process = df_bills[df_bills['gptSummary'].isnull()]
        total = len(rows_to_process)
        
        if total == 0:
            print("[모든 법안에 대한 AI 요약이 이미 존재합니다.]")
            self.output_data = df_bills
            return df_bills

        count = 0
        
        for index, row in rows_to_process.iterrows():
            count += 1
            print(f"현재 진행률: {count}/{total} | {round(count/total*100, 2)}%")

            content, title, bill_id, proposer = row['summary'], row['billName'], row['billNumber'], row['proposers']
            proposer_kind = row['proposerKind'] # '의원', '위원장', '정부'
            
            print('-'*10)
            
            # 1. 'proposerKind' 값을 키로 사용해 prompt_dict에서 직접 템플릿 가져오기
            #    .get()을 사용하여 해당 키가 없는 경우에도 오류 없이 안전하게 처리합니다.
            prompt_template = self.prompt_dict.get(proposer_kind)
            
            if not prompt_template:
                print(f"경고: '{proposer_kind}'에 해당하는 프롬프트 템플릿이 없습니다. (법안: {title})")
                continue

            # 2. 선택된 프롬프트 템플릿 포맷팅
            system_prompt = prompt_template.format(proposer=proposer, title=title)
            
            task = f"\n위 내용은 {title}이야. 이 법률개정안에서 무엇이 달라졌는지 제안이유 및 주요내용을 쉽게 요약해줘."
            print(f"task: {task}")
            print('-'*10)
            
            messages = [
                {"role": "system", "content": system_prompt}, 
                {"role": "user", "content": str(content) + str(task)}
            ]

            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                )
                chat_response = response.choices[0].message.content
                print(f"chatGPT: {chat_response}")
                
                df_bills.loc[index, 'gptSummary'] = chat_response
            except Exception as e:
                print(f"[API 호출 오류] 법안: {title}, 오류: {e}")
                continue

            if count % 5 == 0 and count < total:
                # clear_output(wait=True)
                pass
        
        # clear_output()
        print(f"\n[법안 {count}건 요약 완료됨]")
        print("[AI 내용 요약 완료]")

        self.output_data = df_bills
        return df_bills

    def AI_model_test(date=None, title_model=None, content_model=None):
        pass


# TODO: APISender 구현
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
            
            return response
        except Exception as e:
            print(f"데이터 전송 중 오류 발생: {e}")


# TODO: WorkFlowManager 구현
class WorkFlowManager:
    def __init__(self, mode):
        self.mode = None
        self.mode_list = ['remote', 'local', 'test', 'save', 'fetch']

        load_dotenv()

    def update_bills_data(self, start_date=None, end_date=None, mode=None, age=None):
        """법안 데이터를 수집해 AI 요약 후 API 서버로 전송하는 함수

        Args:
            start_date (str, optional): 시작 날짜 (YYYY-MM-DD 형식). Defaults to None.
            end_date (str, optional): 종료 날짜 (YYYY-MM-DD 형식). Defaults to None.
            mode (str, optional): 실행 모드. Defaults to 'test'. 가능 모드: 'update', 'local', 'test', 'save'.
            age (str, optional): 국회 데이터 수집 대수

        Returns:
            pd.DataFrame: 전송된 데이터프레임
        """
        print("[법안 데이터 수집 및 전송 시작]")

        # 실행 모드 체크
        if mode is None:
            mode = print(input("실행 모드를 선택해주세요. remote | local | test | save | fetch"))
            
            if mode not in ['remote', 'local', 'test', 'save', 'fetch']:
                print("올바른 모드를 선택해주세요. remote | local | test | save | fetch")
                return None
        
        # 데이터 수집 기간 설정
        if start_date is None:
            # DB에 연결하여 현재 가장 최신 법안 날짜 가져오기
            try:
                DBconn = DataBaseManager()
                latest_propose_dt = DBconn.get_latest_propose_data()

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

        bills_content_data = fetcher.fetch_data('bill_content')
        bills_info_data = fetcher.fetch_data('bill_info')

        # 2. 데이터 처리
        processor = DataProcessor(fetcher)
        
        # 법안 데이터 머지
        df_bills = processor.merge_bills_df(bills_content_data, bills_info_data)

        # 중복 데이터 제거
        processor.remove_duplicates(df_bills)

        if len(df_bills) == 0:
            print("새로운 데이터가 없습니다. 코드를 종료합니다.")
            return None

        # AI 요약 컬럼 추가
        processor.add_AI_summary_columns(df_bills)

        # 의원 데이터 처리
        df_bills_congressman = processor.process_congressman_bills(df_bills)

        # 위원장 데이터 처리
        df_bills_chair = processor.process_chairman_bills(df_bills)

        # 정부 데이터 처리
        df_bills_gov = processor.process_gov_bills(df_bills)

        # 발의주체별 법안 데이터 합치기
        df_bills = pd.concat([df_bills_congressman, df_bills_chair, df_bills_gov], ignore_index=True)

        # 3. 데이터 AI 요약 및 전송(모드별 처리)
        payload_name = os.environ.get("PAYLOAD_bills")
        url = os.environ.get("POST_URL_bills")

        summerizer = AISummerizer()
        sender = APISender()

        if mode == 'remote':
            print("[데이터 요약 및 전송 시작]")
            
            # 제목 요약
            summerizer.AI_title_summarize(df_bills)

            # 내용 요약
            # TODO: 내용요약 메서드 구조 개선하고 여기서부터 작업 재개할것
            summerizer.AI_content_summarize(df_bills)


            print("[정당별 법안 발의수 갱신 요청 중...]")
            post_url_party_bill_count = os.environ.get("POST_URL_party_bill_count")
            request_post(post_url_party_bill_count)
            print("[정당별 법안 발의수 갱신 요청 완료]")
            
            print("[의원별 최신 발의날짜 갱신 요청 중...]")
            post_ulr_congressman_propose_date = os.environ.get("POST_URL_congressman_propose_date")
            request_post(post_ulr_congressman_propose_date)
            print("[의원별 최신 발의날짜 갱신 요청 완료]")



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

        mode = 'update' if self.params is None else self.params.get('mode', 'update')

        if mode == 'update':
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

        else:
            print("모드를 선택해주세요. update | local | test | save")

        return df_lawmakers

    def update_bills_timeline(self):
        """의정활동(법안 처리 단계) 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        # 기본 날짜 설정: DB에 저장된 최신 날짜 다음 날부터 오늘까지
        if self.params is None or self.params.get('start_date') is None:
            DBconn = DatabaseManager()
            latest_date = DBconn.get_latest_timeline_date()
            start_date = latest_date.strftime('%Y-%m-%d') if latest_date else (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = self.params.get('start_date')

        if self.params is None or self.params.get('end_date') is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        else:
            end_date = self.params.get('end_date')

        age = None if self.params is None else self.params.get('age')

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

        # 모드 설정
        mode = 'remote'
        if self.params is not None:
            mode = self.params.get('mode', 'remote')

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

        else:
            print("올바른 모드를 선택해주세요. remote | local | test | save")

        return df_stage

    def update_bills_result(self):
        """법안 처리 결과 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        if self.params is None or self.params.get('start_date') is None:
            start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = self.params.get('start_date')

        if self.params is None or self.params.get('end_date') is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        else:
            end_date = self.params.get('end_date')

        age = None if self.params is None else self.params.get('age')

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

        mode = 'remote'
        if self.params is not None:
            mode = self.params.get('mode', 'remote')

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

        else:
            print("올바른 모드를 선택해주세요. remote | local | test | save")

        return df_result

    def update_bills_vote(self):
        """본회의 표결 결과 데이터를 수집하고 모드에 따라 전송 또는 저장하는 메서드"""

        if self.params is None or self.params.get('start_date') is None:
            start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = self.params.get('start_date')

        if self.params is None or self.params.get('end_date') is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        else:
            end_date = self.params.get('end_date')

        age = None if self.params is None else self.params.get('age')

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

        mode = 'remote'
        if self.params is not None:
            mode = self.params.get('mode', 'remote')

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
        else:
            print("올바른 모드를 선택해주세요. remote | local | test | save")

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
        else:
            print("올바른 모드를 선택해주세요. remote | local | test | save")

        return df_vote, df_vote_party

    def update_bills_alternatives(self):
        """대안-법안 관계 데이터를 수집하고 모드에 따라 저장 또는 전송하는 메서드"""

        fetch_mode = 'total'
        if self.params is not None:
            fetch_mode = self.params.get('fetch_mode', 'total')

        if fetch_mode != 'total':
            print("현재는 'total' 모드만 지원합니다.")
            return None

        api_key = os.environ.get("APIKEY_billsContent")
        url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'

        params = {
            'serviceKey': api_key,
            'numOfRows': '100',
            'start_ord': self.params.get('start_ord', os.getenv('AGE')) if self.params else os.getenv('AGE'),
            'end_ord': self.params.get('end_ord', os.getenv('AGE')) if self.params else os.getenv('AGE'),
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

        mode = 'save'
        if self.params is not None:
            mode = self.params.get('mode', 'save')

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
        else:
            df_alternatives.to_csv('bills_alternatives.csv', index=False)
            print('[데이터 저장 완료]')

        return df_alternatives

    def update_commitee_info(self):
        pass