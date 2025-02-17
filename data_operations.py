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

def connect_RDS(host:str, port:int, username:str, password:str, database:str):
    """ RDS 데이터베이스 연결 함수

    Args:
        host (str): _description_
        port (int): _description_
        username (str): _description_
        password (str): _description_
        database (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        conn = pymysql.connect(host=host, 
                               port=port, 
                               user=username, 
                               password=password, 
                               db=database, 
                               use_unicode=True,
                               charset='utf8')
        cursor = conn.cursor()
        print("RDS Connection Succeed")
    
    except Exception as e:
        print("RDS Connection Failed")
        print(e)
        conn = None
    
    return conn, cursor

def get_latest_propose_date(cursor):
    """ RDS 데이터베이스에서 가장 최근의 법안 발의 날짜를 가져오는 함수

    Args:
        cursor (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        query = "SELECT MAX(propose_date) FROM Bill"
        cursor.execute(query)
        result = cursor.fetchone()
        latest_date = result[0]
        return latest_date
    except Exception as e:
        print("Failed to fetch the latest propose_date")
        print(e)
        return None

def get_latest_timeline_date(cursor):
    """ RDS 데이터베이스에서 가장 최근의 법안 처리 날짜를 가져오는 함수

    Args:
        cursor (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        query = "SELECT MAX(status_update_date) FROM BillTimeline"
        cursor.execute(query)
        result = cursor.fetchone()
        latest_date = result[0]
        return latest_date
    except Exception as e:
        print("Failed to fetch the latest status_update_date")
        print(e)
        return None
        
def fetch_bills_content(start_date:str=None, end_date:str=None, age=None):     
    """ 법안 주요내용 데이터 수집 함수

    Args:
        start_date (str, optional): _description_. Defaults to None.
        end_date (str, optional): _description_. Defaults to None.
    """
    
    load_dotenv()
     # 기본 날짜 설정
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')  
    if age is None:
        age = os.environ.get("AGE")
    
    # 환경 변수로부터 API 키 및 국회 회기 정보 로드
    
    api_key = os.environ.get("APIKEY_billsContent")
    url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'
    params = {
        'serviceKey': api_key,
        'numOfRows': '100',
        'start_ord': age,
        'end_ord': age,
        'start_propose_date': start_date,
        'end_propose_date': end_date
    }

    # 수집하는 날짜 범위 출력
    print(f"[{start_date} ~ {end_date} 의안주요내용 데이터 수집 시작]")
    
    # 데이터 수집 시작
    all_data = []
    pageNo = 1
    processing_count = 0
    max_retry=3

    start_time = time.time()

    while True:
        params.update({'pageNo': str(pageNo)})
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            try:
                root = ElementTree.fromstring(response.content)
                items = root.find('body').find('items')
                
                if items is None or len(items) == 0:
                    # print("No more data available.")
                    break
                
                data = [{child.tag: child.text for child in item} for item in items]
                all_data.extend(data)
                
                # print(f"Page {pageNo} processed. {len(data)} items added. total: {len(all_data)}")
                processing_count += 1
                
            except ElementTree.ParseError:
                print(f"XML Parsing Error: {response.text}")
                max_retry -= 1
            except Exception as e:
                print(f"Unexpected Error: {e}")
                max_retry -= 1
        else:
            print(f"Error Code: {response.status_code} (Page {pageNo})")
            max_retry -= 1
        
        if max_retry <= 0:
            print("Maximum retry reached. Exiting...")
            break
        
        pageNo += 1

    # 데이터프레임 생성
    df_billsContent = pd.DataFrame(all_data)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
    print(f"[{len(df_billsContent)} 개의 법안 수집됨.]")
    
    # 수집한 데이터가 없으면 종료
    if len(df_billsContent) == 0:
        return None

    # 유지할 컬럼 목록
    columns_to_keep = [
        'proposeDt', # 발의일자
        'billNo', # 법안번호
        # 'billName', # 법안명
        'summary', # 주요내용
        'procStageCd', # 현재 처리 단계
        # 'generalResult' # 처리 결과
        'proposerKind'
    ]

    # 지정된 컬럼만 유지하고 나머지 제거
    df_billsContent = df_billsContent[columns_to_keep]

    # 'summary' 컬럼에 결측치가 있는 행 제거
    df_billsContent = df_billsContent.dropna(subset=['summary'])

    # 인덱스 재설정
    df_billsContent.reset_index(drop=True, inplace=True)

    # 컬럼 이름 변경
    df_billsContent.rename(columns={
        "proposeDt": "proposeDate",
        "billNo": "billNumber",
        "summary": "summary",
        "procStageCd": "stage"
        }, inplace=True)

    print(f"[결측치 처리 완료. {len(df_billsContent)} 개의 법안 수집됨.]")
    print("\n발의일자별 수집한 데이터 수 :")
    print(f"{df_billsContent['proposeDate'].value_counts()}")    

    return df_billsContent

# def fetch_bills_info(start_date=None, end_date=None):
#     """법안 정보 데이터 수집 함수 (JSON 데이터 파싱)
    
#     Args:
#         start_date (str): 수집을 시작할 날짜 (포맷: 'YYYY-MM-DD')
#         end_date (str): 수집을 종료할 날짜 (포맷: 'YYYY-MM-DD')

#     Returns:
#         pd.DataFrame: 수집된 법안 정보 데이터
#     """
    
#     # 기본 날짜 설정
#     if end_date is None:
#         end_date = datetime.now().date()
#     if start_date is None:
#         start_date = end_date - timedelta(days=1)
    
#     # 날짜가 문자열로 입력된 경우 datetime 객체로 변환
#     if isinstance(start_date, str):
#         start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
#     if isinstance(end_date, str):
#         end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

#     # API 정보 설정
#     load_dotenv()
    
#     api_key = os.environ.get("APIKEY_billsInfo")
#     url = "https://open.assembly.go.kr/portal/openapi/ALLBILL"
    
#     all_data = []
#     current_date = start_date
    
#     print(f"\n[{start_date}부터 {end_date}까지의 법안 데이터 수집 중]")
#     start_time = time.time()
    
#     while current_date <= end_date:
#         pageNo = 1  # 페이지 번호 초기화
#         while True:
#             # 현재 날짜와 페이지에 대한 요청
#             params = {
#                 "Key": api_key,
#                 "Type": "json",
#                 "pSize": 100,
#                 "pIndex": pageNo,
#                 "PPSL_DT": current_date.strftime('%Y-%m-%d')
#             }
            
#             try:
#                 response = requests.get(url, params=params, timeout=10)
#                 response.raise_for_status()  # HTTP 오류 확인
                
#                 # JSON 데이터 파싱
#                 response_data = response.json()
                
#                 items = response_data.get("ALLBILL", [])
                
#                 if len(items) > 1:  # 데이터가 있을 경우 처리
#                     data = items[1].get('row', [])
#                     if data:
#                         all_data.extend(data)
#                         print(f"{current_date} | 페이지 {pageNo} | 수집된 항목 수: {len(data)}")
#                     else:
#                         print(f"{current_date} 모든 데이터 수집 완료.")
#                         break  # 더 이상 페이지가 없으므로 종료
#                 else:
#                     # print(f"{current_date}에 데이터가 없습니다.")
#                     break  # 더 이상 데이터가 없으므로 종료

#             except requests.exceptions.RequestException as e:
#                 print(f"요청 오류: {e}")
#                 break  # 오류 발생 시 종료
#             except json.JSONDecodeError:
#                 print(f"JSON 파싱 오류: {response.text}")
#                 break  # 파싱 오류 발생 시 종료
#             except Exception as e:
#                 print(f"예상치 못한 오류: {e}")
#                 break  # 기타 오류 발생 시 종료
            
#             pageNo += 1  # 다음 페이지로 이동
        
#         # 다음 날짜로 이동
#         current_date += timedelta(days=1)
    
#     # DataFrame 생성
#     df_billsInfo = pd.DataFrame(all_data)
    
#     end_time = time.time()
#     total_time = end_time - start_time
#     print(f"다운로드 완료! 총 소요 시간: {total_time:.2f}초")
    
#     # 데이터가 없으면 종료
#     if df_billsInfo.empty:
#         print("수집한 데이터가 없습니다.")
#         return None
    
#     print(f"[{len(df_billsInfo)}개의 법안 정보 데이터가 수집되었습니다.]")
    
#     # 컬럼 필터링
#     columns_to_keep = ['ERACO', 'BILL_ID', 'BILL_NO', 'BILL_NM', 'PPSR_NM', 'JRCMIT_NM']
#     df_billsInfo = df_billsInfo[columns_to_keep]
    
#     # 컬럼명 변경
#     column_mapping = {
#         'ERACO': 'assemblyNumber',
#         'BILL_ID': 'billId',
#         'BILL_NO': 'billNumber',
#         'BILL_NM': 'billName',
#         'PPSR_NM': 'proposers',
#         'JRCMIT_NM': 'committee'
#     }    
#     df_billsInfo.rename(columns=column_mapping, inplace=True)
    
#     print("컬럼 필터링 및 컬럼명 변경 완료")
    
#     # 정규 표현식을 사용하여 이름을 추출하는 함수 정의
#     def extract_names(proposer_str):
#         return re.findall(r'[가-힣]+(?=의원)', proposer_str)

#     # 새로운 컬럼 rstProposerNameList에 이름 리스트를 추가
#     df_billsInfo['rstProposerNameList'] = df_billsInfo['proposers'].apply(extract_names)
    
#     df_billsInfo['assemblyNumber'] = df_billsInfo['assemblyNumber'].str.replace(r'\D', '', regex=True)
    
#     return df_billsInfo

def fetch_bills_info(df_bills): #API 요청인자 변경으로 인해 수정된 코드

    # API 정보 설정
    load_dotenv()
    
    api_key = os.environ.get("APIKEY_billsInfo")
    url = "https://open.assembly.go.kr/portal/openapi/ALLBILL"
    
    all_data = []
    
    print(f"\n[법안 정보 데이터 수집 중]")
    start_time = time.time()
    
    for row in tqdm(df_bills.itertuples(), total=len(df_bills)):
        # 현재 날짜와 페이지에 대한 요청
        params = {
            "Key": api_key,
            "Type": "json",
            "pSize": 5,
            "pIndex": 1,
            "BILL_NO": row.billNumber
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()  # HTTP 오류 확인
            
            # JSON 데이터 파싱
            response_data = response.json()
            
            items = response_data.get("ALLBILL", [])
            
            if len(items) > 1:  # 데이터가 있을 경우 처리
                data = items[1].get('row', [])
                if data:
                    all_data.extend(data)
                    # print(f"{current_date} | 페이지 {pageNo} | 수집된 항목 수: {len(data)}")
                else:
                    # print(f"{current_date} 모든 데이터 수집 완료.")
                    break  # 더 이상 페이지가 없으므로 종료
            else:
                # print(f"{current_date}에 데이터가 없습니다.")
                break  # 더 이상 데이터가 없으므로 종료

        except requests.exceptions.RequestException as e:
            print(f"요청 오류: {e}")
            break  # 오류 발생 시 종료
        except json.JSONDecodeError:
            print(f"JSON 파싱 오류: {response.text}")
            break  # 파싱 오류 발생 시 종료
        except Exception as e:
            print(f"예상치 못한 오류: {e}")
            break  # 기타 오류 발생 시 종료
    

    
    # DataFrame 생성
    df_billsInfo = pd.DataFrame(all_data)
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"다운로드 완료! 총 소요 시간: {total_time:.2f}초")
    
    # 데이터가 없으면 종료
    if df_billsInfo.empty:
        print("수집한 데이터가 없습니다.")
        return None
    
    print(f"[{len(df_billsInfo)}개의 법안 정보 데이터가 수집되었습니다.]")
    
    # 컬럼 필터링
    columns_to_keep = ['ERACO', 'BILL_ID', 'BILL_NO', 'BILL_NM', 'PPSR_NM', 'JRCMIT_NM']
    df_billsInfo = df_billsInfo[columns_to_keep]
    
    # 컬럼명 변경
    column_mapping = {
        'ERACO': 'assemblyNumber',
        'BILL_ID': 'billId',
        'BILL_NO': 'billNumber',
        'BILL_NM': 'billName',
        'PPSR_NM': 'proposers',
        'JRCMIT_NM': 'committee'
    }    
    df_billsInfo.rename(columns=column_mapping, inplace=True)
    
    print("컬럼 필터링 및 컬럼명 변경 완료")
    
    # 정규 표현식을 사용하여 이름을 추출하는 함수 정의
    def extract_names(proposer_str):
        return re.findall(r'[가-힣]+(?=의원)', proposer_str)

    # 새로운 컬럼 rstProposerNameList에 이름 리스트를 추가
    df_billsInfo['rstProposerNameList'] = df_billsInfo['proposers'].apply(extract_names)
    
    df_billsInfo['assemblyNumber'] = df_billsInfo['assemblyNumber'].str.replace(r'\D', '', regex=True)
    
    return df_billsInfo

# def fetch_bills_proposers(df_bills):
#     """법안 제안자 정보 수집 함수 (BILL_ID 기반 요청 및 모든 페이지 수집)
    
#     Args:
#         df_bills (pd.DataFrame): 법안 정보가 담긴 데이터프레임 (billId 컬럼 포함)

#     Returns:
#         pd.DataFrame: 수집된 법안 제안자 정보 데이터
#     """
    
#     # API 정보 설정
#     load_dotenv()
    
#     api_key = os.environ.get("APIKEY_billsInfo")
#     url = "https://open.assembly.go.kr/portal/openapi/nzmimeepazxkubdpn"
    
    
#     all_proposer_data = []
    
#     print("\n[법안 제안자 정보 데이터 수집 중]")
#     start_time = time.time()
    
#     # df_bills에서 billId 값을 순차적으로 사용하여 요청
#     for bill_id in tqdm(df_bills['billId']):
#         pageNo = 1  # 페이지 번호 초기화
#         while True:
#             params = {
#                 "Key": api_key,
#                 "Type": "json",
#                 "BILL_ID": bill_id,
#                 "pSize": 100,     # 페이지 당 데이터 수
#                 "pIndex": pageNo,   # 페이지 번호
#                 "AGE" : os.environ.get("AGE")
#             }
            
#             try:
#                 response = requests.get(url, params=params, timeout=10)
#                 response.raise_for_status()  # HTTP 오류 확인
                
#                 # JSON 데이터 파싱
#                 response_data = response.json()
                
#                 # print(response_data)
                
#                 items = response_data.get("nzmimeepazxkubdpn", [])
                
#                 if len(items) > 1:  # 데이터가 있을 경우 처리
#                     proposer_data = items[1].get('row', [])
#                     if proposer_data:
#                         all_proposer_data.extend(proposer_data)
#                         # print(f"billId: {bill_id}, 페이지: {pageNo} | 수집된 항목 수: {len(proposer_data)}")
#                     else:
#                         # print(f"billId: {bill_id}, 페이지: {pageNo}에 제안자 정보가 없습니다.")
#                         break  # 더 이상 데이터가 없으면 종료
#                 else:
#                     # print(f"billId: {bill_id}, 페이지: {pageNo}에 데이터가 없습니다.")
#                     break  # 더 이상 데이터가 없으면 종료

#             except requests.exceptions.RequestException as e:
#                 print(f"요청 오류: {e}")
#                 break  # 오류 발생 시 종료
#             except json.JSONDecodeError:
#                 print(f"JSON 파싱 오류: {response.text}")
#                 break  # 파싱 오류 발생 시 종료
#             except Exception as e:
#                 print(f"예상치 못한 오류: {e}")
#                 break  # 기타 오류 발생 시 종료
            
#             # 다음 페이지로 이동
#             pageNo += 1
    
#     # DataFrame 생성
#     df_proposers = pd.DataFrame(all_proposer_data)
    
#     end_time = time.time()
#     total_time = end_time - start_time
#     print(f"다운로드 완료! 총 소요 시간: {total_time:.2f}초")
    
#     # 데이터가 없으면 None 반환
#     if df_proposers.empty:
#         return None
    
#     print(f"[{len(df_proposers)}개의 제안자 정보 데이터가 수집되었습니다.]")
    
#     # 필요한 컬럼만 선택적으로 추출 (원하는 경우 수정 가능)
#     columns_to_keep = ['BILL_ID', 'RST_PROPOSER', 'PUBL_PROPOSER']
#     df_proposers = df_proposers[columns_to_keep].copy()
    
#     column_mapping = {
#         'BILL_ID': 'billId',
#         'RST_PROPOSER': 'rstProposerNameList',
#         'PUBL_PROPOSER': 'publicProposers'
#     }
#     df_proposers.rename(columns=column_mapping, inplace=True)
    
#     print("[컬럼 필터링 완료]")
    
#     return df_proposers

def fetch_bills_coactors(df_bills):
    """billId를 사용하여 각 법안의 공동 발의자 명단을 수집하는 함수

    Args:
        df_bills (pd.DataFrame): billId 컬럼을 포함한 법안 정보 데이터프레임

    Returns:
        pd.DataFrame: billId, ProposersCode 컬럼을 가진 데이터프레임
    """
    coactors_data = []
    
    # 국회의원 데이터 가져오기
    df_lawmakers = fetch_lawmakers_data()

    # 각 법안의 billId에 대해 공동 발의자 정보를 수집
    for billId in tqdm(df_bills['billId']):
        # URL 생성
        url = f"http://likms.assembly.go.kr/bill/coactorListPopup.do?billId={billId}"
        
        # HTML 가져오기
        try:
            response = requests.get(url)
            response.raise_for_status()  # 요청에 실패한 경우 예외 발생
        except requests.RequestException as e:
            print(f"Failed to fetch data for billId {billId}: {e}")
            continue
        
        # 응답으로부터 HTML 내용 가져오기
        html_content = response.text
        
        # BeautifulSoup을 이용하여 HTML 파싱
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 공동 발의자 명단이 있는 'links' 클래스 검색
        coactors_section = soup.find('div', {'class': 'links textType02 mt20'})
        
        # 공동 발의자 섹션이 없는 경우
        if coactors_section is None:
            print(f"공동 발의자 명단을 찾을 수 없습니다 for billId {billId}.")
            continue
        
        # 공동 발의자 명단에서 <a> 태그 내 텍스트를 추출
        for a_tag in coactors_section.find_all('a'):
            coactor_text = a_tag.get_text(strip=True)  # 공동 발의자의 이름, 소속 정당, 한자 이름 포함
            # 정규표현식을 사용하여 이름, 소속 정당, 한자 이름 추출
            match = re.match(r'(.+?)\((.+?)/(.+?)\)', coactor_text)
            if match:
                proposer_name, proposer_party, proposer_hj_name = match.groups()
                # 공동 발의자 데이터를 리스트에 추가
                coactors_data.append([billId, proposer_name, proposer_party, proposer_hj_name])

    # DataFrame 생성 (billId, ProposerName, ProposerParty, ProposerHJName 컬럼 포함)
    df_coactors = pd.DataFrame(coactors_data, columns=['billId', 'ProposerName', 'ProposerParty', 'ProposerHJName'])

    # publicProposerIdList 추가 (df_lawmakers에서 해당 의원의 MONA_CD 값을 가져옴)
    proposer_codes = []
    for _, row in df_coactors.iterrows():
        # 공동 발의자 정보와 일치하는 국회의원 정보를 df_lawmakers에서 검색
        match = df_lawmakers[(df_lawmakers['HG_NM'] == row['ProposerName']) &
                             (df_lawmakers['POLY_NM'] == row['ProposerParty']) &
                             (df_lawmakers['HJ_NM'] == row['ProposerHJName'])]
        # 일치하는 국회의원이 있는 경우 MONA_CD 값을 추가, 없는 경우 None 추가
        if not match.empty:
            proposer_codes.append(match['MONA_CD'].values[0])
        else:
            proposer_codes.append(None)

    # publicProposerIdList 컬럼을 df_coactors에 추가
    df_coactors['publicProposerIdList'] = proposer_codes

    # billId와 publicProposerIdList 컬럼만 선택하여 새로운 DataFrame 생성
    df_coactors = df_coactors[['billId', 'publicProposerIdList', 'ProposerName']]

    # 동일한 billId에 대해 publicProposerIdList, ProposerName을 리스트 형태로 합치기
    df_coactors = df_coactors.groupby('billId').agg({
    'publicProposerIdList': lambda x: x.dropna().tolist(),
    'ProposerName': lambda x: x.dropna().tolist()
    }).reset_index()
    
    return df_coactors

def process_by_proposer_type(df_bills):
    """법안 발의자 유형별로 데이터를 그룹화하는 함수

    Args:
        df_bills (pd.DataFrame): 법안 데이터

    Returns:
        dict: 법안 발의자 유형별로 그룹화된 데이터
    """
    
    df_bills_congressman = df_bills[df_bills['proposerKind'] == '의원'].copy()
    df_bills_chair = df_bills[df_bills['proposerKind'] == '위원장'].copy()
    df_bills_gov = df_bills[df_bills['proposerKind'] == '정부'].copy()
    
    if(len(df_bills_congressman) == 0 and len(df_bills_chair) == 0):
        print("의원 혹은 위원장이 발의한 법안이 없습니다. 코드를 종료합니다.")
        return pd.DataFrame()

    if len(df_bills_chair) > 0: # 위원장 발의 법안이 존재하는 경우

        print("위원장 발의 법안 존재 - 대안 관계 데이터 수집")
        df_alternatives = fetch_bills_alternatives(df_bills_chair)


        

    # df_bills_congressman 처리
    
    # df_bills_congressman에 발의자 정보 컬럼 머지
    print("\n[의원 발의자 데이터 수집 및 병합 중...]")
    df_coactors = fetch_bills_coactors(df_bills_congressman)  
    df_bills_congressman = pd.merge(df_bills_congressman, df_coactors, on='billId', how='inner')

    print("[의원 발의자 데이터 수집 및 병합 완료]")
    
    def get_proposer_codes(row):
        name_list_length = len(row['rstProposerNameList'])
        return row['publicProposerIdList'][:name_list_length]

    # 새로운 컬럼 rstProposerIdList에 publicProposerIdList 리스트에서 슬라이싱한 값 추가
    print(df_bills_congressman.info())
    df_bills_congressman['rstProposerIdList'] = df_bills_congressman.apply(get_proposer_codes, axis=1)

    # print("\n[대표발의정당 리스트 추가 중...]")
    
    # 의원 데이터 가져오기
    # df_lawmakers = fetch_lawmakers_data()
    
    # # 대표발의정당 리스트화 함수 - rstProposerNameList를 , 기준으로 분리하여 각각의 이름에 대해 df_lawmakers의 POLY_NM 값 가져오기
    # def get_party_names(proposers, lawmakers_df):
    #     proposers = proposers.split(',')
    #     party_names = []
    #     for proposer in proposers:
    #         party_name = lawmakers_df.loc[lawmakers_df['HG_NM'] == proposer, 'POLY_NM']
    #         if not party_name.empty:
    #             party_names.append(party_name.values[0])
    #         else:
    #             party_names.append(None)
    #     return party_names

    # 대표발의정당 리스트 컬럼 생성
    # df_bills_congressman['rstProposerPartyNameList'] = df_bills_congressman['rstProposerNameList'].apply(get_party_names, lawmakers_df=df_lawmakers)    
    
    # 대표발의자, 공동발의자 컬럼 리스트화
    # df_bills_congressman['publicProposers'] = df_bills_congressman['publicProposers'].apply(lambda x: [] if pd.isna(x) else x.split(',') if isinstance(x, str) else [x])
    # df_bills_congressman['rstProposerNameList'] = df_bills_congressman['rstProposerNameList'].apply(lambda x: [] if pd.isna(x) else x.split(',') if isinstance(x, str) else [x])

    # print("[대표발의정당 리스트 추가 완료]")
    
    # df_bills_chair의 billName에서 (대안) 제거
    df_bills_chair['billName'] = df_bills_chair['billName'].str.replace(r'\(대안\)', '', regex=True)
    
    #df_bills_chair에 발의자 정보 빈 리스트로 추가
    # df_bills_chair['rstProposerNameList'] = ""
    # df_bills_chair['rstProposerPartyNameList'] = ""
    # df_bills_chair['publicProposers'] = ""
    
    # df_bills_gov에 발의자 정보 빈 리스트로 추가
    # df_bills_gov['rstProposerNameList'] = ""
    # df_bills_gov['rstProposerPartyNameList'] = ""
    # df_bills_gov['publicProposers'] = ""
    
    # 모든 데이터프레임을 하나로 합치기
    print("\n[모든 데이터프레임 병합 중...]")
    df_combined = pd.concat([df_bills_congressman, df_bills_chair], ignore_index=True)
    
    print(f"[병합된 데이터프레임 크기: {len(df_combined)}행]")
    print(f"[의원 발의: {len(df_bills_congressman)}행, 위원장 발의: {len(df_bills_chair)}행]")
    
    # 제외할 컬럼 목록
    columns_to_drop = ['rstProposerNameList', 'ProposerName']
    df_combined.drop(columns=columns_to_drop, inplace=True)
    
    return df_combined


def merge_bills_df(df_bills_content, df_bills_info):
    
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
    
    df_bills['briefSummary'] = None
    df_bills['gptSummary'] = None
    print("\n[AI 요약 데이터 컬럼 추가 완료]")
    
    print(df_bills['proposeDate'].value_counts())
    
    return df_bills

def remove_duplicates(df):

    print("\n[DB와의 중복 데이터 제거 중...]")

    # 법안 ID 리스트 추출
    bill_ids = df['billId'].tolist()

    # 기존 법안 ID 조회
    def get_existing_bill_ids(conn, cursor, bill_ids):
        # SQL query to fetch existing bill IDs
        format_strings = ','.join(['%s'] * len(bill_ids))
        cursor.execute(f"SELECT {'bill_id'} FROM Bill WHERE {'bill_id'} IN ({format_strings})", tuple(bill_ids))
        result = cursor.fetchall()
        # Extract IDs from the result
        existing_ids = [row[0] for row in result]
        cursor.close()
        return existing_ids

    load_dotenv()

    # RDS 연결
    host = os.environ.get("host")
    port = int(os.environ.get("port"))
    username = os.environ.get("username")
    password = os.environ.get("password")
    database = os.environ.get("database")

    conn, cursor = connect_RDS(host, port, username, password, database)

    try:
        # 기존 법안 ID 조회
        existing_ids = get_existing_bill_ids(conn, cursor, bill_ids)
    finally:
        # RDS 연결 종료
        conn.close()

    # 4. 데이터프레임에서 DB에 존재하지 않는 법안 ID만 남기기
    df_bills = df[~df['billId'].isin(existing_ids)]
    
    # 중복 처리 결과 출력
    print(f"[총 {len(df)}개의 법안 데이터 중 {len(df_bills)}개의 새로운 법안 데이터 발견됨.]")

    print(df_bills['proposeDate'].value_counts())

    return df_bills

# AI 제목 요약
def AI_title_summarize(df_bills, model=None):
    
    load_dotenv()
    
    client = OpenAI(
        api_key=os.environ.get("APIKEY_OPENAI"),  # this is also the default, it can be omitted
    )
    
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
    return df_bills

# AI 본문 요약
def AI_content_summarize(df_bills, model=None):

    load_dotenv()

    client = OpenAI(
        api_key=os.environ.get("APIKEY_OPENAI"),  # this is also the default, it can be omitted
    )

    if model == None:
        model = os.environ.get("CONTENT_SUMMARIZATION_MODEL")

    print("\n[AI 내용 요약 진행 중...]")
    
    # 'gptSummary' 컬럼이 공백이 아닌 경우에만 요약문을 추출하여 해당 컬럼에 저장
    total = df_bills['gptSummary'].isnull().sum()
    count = 0
    show_count = 0

    for index, row in df_bills.iterrows():
        count += 1
        print(f"현재 진행률: {count}/{total} | {round(count/total*100, 2)}%")

        content, title, id, proposer = row['summary'], row['billName'], row['billNumber'], row['proposers']
        
        print('-'*10)
        if not pd.isna(row['gptSummary']):
            print(f"{title}에 대한 요약문이 이미 존재합니다.")
            # clear_output()
            continue  
            # 이미 'SUMMARY', 'gptSummary' 컬럼에 내용이 있으면 건너뜁니다

        task = f"\n위 내용은 {title}이야. 이 법률개정안에서 무엇이 달라졌는지 쉽게 요약해줘."
        print(f"task: {task}")
        print('-'*10)

        messages = [
            {"role": "system",
            "content": f"너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"{proposer} 의원이 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1. 2. 3. 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해."}, 
            # "content": f"너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"{proposer} 의원이 발의한 {title}은 ~을 위한 법안입니다. 이 법안의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1. 2. 3. 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해."},              
            {"role": "user", "content": str(content) + str(task)}
        ]
        
        response = client.chat.completions.create(
            model=model,
            messages=messages,
        )
        chat_response = response.choices[0].message.content

        print(f"chatGPT: {chat_response}")

        # 추출된 요약문을 'gptSummary' 컬럼에 저장
        df_bills.loc[df_bills['billNumber'] == id, 'gptSummary'] = chat_response
        show_count += 1

        if show_count % 5 == 0:
            clear_output()
    
    print(f"[법안 {count}건 요약 완료됨]")

    clear_output()
    
    print("[AI 내용 요약 완료]")
    return df_bills

def fetch_lawmakers_data():
    """
    국회의원 데이터를 API로부터 가져와서 DataFrame으로 반환하는 함수.
    API 키와 URL은 함수 내부에서 정의되며, 모든 페이지를 처리합니다.

    Returns:
    - df_lawmakers: pandas.DataFrame, 수집된 국회의원 데이터
    """
    api_key = '946323ab4a694ab580186ad13e821de5'
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

    return df_lawmakers

def request_post(url=None):
    
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

def send_data(data, url, payload_name):
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


# def add_proposer_columns(df_bills):
#     print("\n[대표발의정당 리스트 추가 중...]")
    
#     # 의원 데이터 가져오기
#     df_lawmakers = fetch_lawmakers_data()
    
#     # DataFrame의 복사본을 만들어 작업
#     df_bills = df_bills.copy()
    
#     # 대표발의정당 리스트화 함수 - rstProposerNameList를 , 기준으로 분리하여 각각의 이름에 대해 df_lawmakers의 POLY_NM 값 가져오기
#     def get_party_names(proposers, lawmakers_df):
#         proposers = proposers.split(',')
#         party_names = []
#         for proposer in proposers:
#             party_name = lawmakers_df.loc[lawmakers_df['HG_NM'] == proposer, 'POLY_NM']
#             if not party_name.empty:
#                 party_names.append(party_name.values[0])
#             else:
#                 party_names.append(None)
#         return party_names

#     # 대표발의정당 리스트 컬럼 생성
#     # df_bills['rstProposerPartyNameList'] = df_bills['rstProposerNameList'].apply(get_party_names, lawmakers_df=df_lawmakers)    
    
#     # 대표발의자, 공동발의자 컬럼 리스트화
#     df_bills['publicProposers'] = df_bills['publicProposers'].apply(lambda x: [] if pd.isna(x) else x.split(',') if isinstance(x, str) else [x])
#     df_bills['rstProposerNameList'] = df_bills['rstProposerNameList'].apply(lambda x: [] if pd.isna(x) else x.split(',') if isinstance(x, str) else [x])

#     print("[대표발의정당 리스트 추가 완료]")
#     return df_bills

def update_bills_data(start_date=None, end_date=None, mode=None, age=None):
    """법안 데이터를 수집해 AI 요약 후 API 서버로 전송하는 함수

    Args:
        start_date (str, optional): 시작 날짜 (YYYY-MM-DD 형식). Defaults to None.
        end_date (str, optional): 종료 날짜 (YYYY-MM-DD 형식). Defaults to None.
        mode (str, optional): 실행 모드. Defaults to 'test'. 가능 모드: 'update', 'local', 'test', 'save'.

    Returns:
        pd.DataFrame: 전송된 데이터프레임
    """
    
    print("[법안 데이터 수집 및 전송 시작]")
    
    if mode is None:
        mode = print(input("실행 모드를 선택해주세요. remote | local | test | save | fetch"))
        
        if mode not in ['remote', 'local', 'test', 'save', 'fetch']:
            print("올바른 모드를 선택해주세요. remote | local | test | save | fetch")
            return None

    if start_date is None:
        load_dotenv()

        host = os.environ.get("host")
        port = int(os.environ.get("port"))
        username = os.environ.get("username")
        password = os.environ.get("password")
        database = os.environ.get("database")

        conn, cursor = connect_RDS(host, port, username, password, database)

        if conn is not None and cursor is not None:
            latest_PROPOSE_DT = get_latest_propose_date(cursor)
            if latest_PROPOSE_DT:
                print("The latest propose_date is:", latest_PROPOSE_DT)
            cursor.close()
            conn.close()
            
        start_date = latest_PROPOSE_DT
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    if age is None:
        age = os.getenv("AGE")

    df_billsContent = fetch_bills_content(start_date, end_date, age)
    # df_billsInfo = fetch_bills_info(start_date, end_date)
    df_billsInfo = fetch_bills_info(df_billsContent)
    df_bills = merge_bills_df(df_billsContent, df_billsInfo)
    
    # 중복 데이터 제거
    if mode != 'fetch':
        print("[중복 데이터 제거 중...]")  
        df_bills = remove_duplicates(df_bills)
            
    if len(df_bills) == 0:
        print("새로운 데이터가 없습니다. 코드를 종료합니다.")
        return None
    
    df_bills = process_by_proposer_type(df_bills)

    if len(df_bills) == 0:
        print("의원 혹은 위원장 발의 법안 데이터가 없습니다. 코드를 종료합니다.")
        return None
    
    df_bills['proposeDate'].value_counts()
    
    load_dotenv()
    payload_name = os.environ.get("PAYLOAD_bills")
    url = os.environ.get("POST_URL_bills")
    
    if mode == 'remote':
        print("[데이터 요약 및 전송 시작]")
                
        # 날짜별로 그룹핑
        grouped = df_bills.groupby('proposeDate')
        total_dates = len(grouped)

        successful_dates = []
        failed_dates = []
        
        for date, group in grouped:
            print(f"[{date} 데이터 처리 중]")
            try:
                group = AI_title_summarize(group)
                group = AI_content_summarize(group)
                send_data(group, url, payload_name)
                print(f"[{date} 데이터 전송 완료]")
                successful_dates.append(date)
            except Exception as e:
                print(f"[{date} 데이터 처리 중 오류 발생: {e}]")
                failed_dates.append(date)
                exit()

        print("[AI 요약 및 전송 완료]")
        print(f"전송 성공한 날짜: {len(successful_dates)} / 전체 날짜: {total_dates}")
        
        if len(failed_dates) > 0:
            print("전송 실패한 날짜:")
            for failed_date in failed_dates:
                print(f"- {failed_date}")
        else:
            print("모든 날짜의 데이터가 성공적으로 전송되었습니다.")
            
        print("[정당별 법안 발의수 갱신 요청 중...]")
        post_url_party_bill_count = os.environ.get("POST_URL_party_bill_count")
        request_post(post_url_party_bill_count)
        print("[정당별 법안 발의수 갱신 요청 완료]")
        
        print("[의원별 최신 발의날짜 갱신 요청 중...]")
        post_ulr_congressman_propose_date = os.environ.get("POST_URL_congressman_propose_date")
        request_post(post_ulr_congressman_propose_date)
        print("[의원별 최신 발의날짜 갱신 요청 완료]")
            
    elif mode == 'remote test':
        df_bills = df_bills[:5]
        df_bills = AI_title_summarize(df_bills)
        df_bills = AI_content_summarize(df_bills)
        try:
            send_data(df_bills, url, payload_name)
        except Exception as e:
                print(f"[{date} 데이터 처리 중 오류 발생: {e}]")
                failed_dates.append(date)
                exit()

    elif mode == 'local':
        print("[로컬 모드 : AI 요약 생략 및 로컬 DB에 전송]")
        df_bills['gptSummary'] = ""
        df_bills['briefSummary'] = ""

        #url의 api.lawdigest.net 부분을 localhost:8080으로 변경
        url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
        send_data(df_bills, url, payload_name)
    
    elif mode == 'test':
        print("[테스트 모드 : 데이터 요약 및 전송 생략]")
    
    elif mode == 'save':
        df_bills.to_csv('df_bills.csv', index=False)
        df_bills.to_csv('df_bills.csv', index=False)
        
    elif mode == 'fetch':
        print("[데이터 수집 모드: 중복 데이터 제거 없이 데이터를 수집합니다.]")
    
    else:
        print("올바른 모드를 선택해주세요. remote | local | test | save | fetch")
    
    return df_bills

def ai_model_test(date=None, title_model=None, content_model=None):
    
    if date is None:
        # 어제 날짜를 기본값으로 설정
        date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        
    start_date = date
    end_date = date
    
    if title_model is None:
        title_model = input("제목 요약 AI 모델을 선택해주세요.(건너뛰려면 n)")
    
    if title_model == 'n':
        print("제목 요약을 생략합니다.")
        title_model = None
    else:
        print(f'제목 요약 모델 : {title_model}')
            
    
    if content_model is None:
        content_model = input("내용 요약 AI 모델을 선택해주세요.(건너뛰려면 n)")
    
    if content_model == 'n':
        print("내용 요약을 생략합니다.")
        content_model = None
    else:
        print(f'내용 요약 모델 : {content_model}')
        
    
    if title_model is None and content_model is None:
        print("테스트를 진행하려면 제목 요약 또는 내용 요약 중 하나를 선택해주세요.")
        return None
    
    df_billsContent = fetch_bills_content(start_date, end_date)
    df_billsInfo = fetch_bills_info(start_date, end_date)
    df_bills = merge_bills_df(df_billsContent, df_billsInfo)
    
    if len(df_bills) == 0:
        print("새로운 데이터가 없습니다. 코드를 종료합니다.")
        return None
    
    print(df_bills['proposeDate'].value_counts())
    
    df_bills = df_bills.head(5)
    if title_model:
        df_bills = AI_title_summarize(df_bills, title_model)
    if content_model:
        df_bills = AI_content_summarize(df_bills, content_model)
    
    for bill in df_bills.itertuples():
        print(f"{bill.billName} ({bill.proposeDate})")
        print(f"제목 요약: {bill.briefSummary}")
        print(f"내용 요약: {bill.gptSummary}")
        print('-'*10)
    
    return df_bills

def update_lawmakers_data(mode):
    
    print("\n[의원 데이터 수집 시작]")

    df_lawmakers = fetch_lawmakers_data()
    
    columns_to_drop = [
        'ENG_NM', # 영문이름
        'HJ_NM', # 한자이름
        'BTH_GBN_NM', # 음력/양력 구분
        'ELECT_GBN_NM', #선거구구분. '선거구' 컬럼으로 지역구/비례를 구분할 수 있으므로 제거
        'STAFF', # 보좌관
        'CMITS', # 소속위원회목록. '대표 위원회' 컬럼이 있으므로 제거
        'SECRETARY', # 비서관 
        'SECRETARY2', # 비서
        # 'ASSEM_ADDR', # 사무실 호실
        'JOB_RES_NM', # 직위
    ]

    df_lawmakers = df_lawmakers.drop(columns=columns_to_drop)

    # UNITS 컬럼의 값에서 숫자만 추출
    df_lawmakers['UNITS'] = df_lawmakers['UNITS'].str.extract(r'(\d+)(?=\D*$)').astype(int)

    column_mapping = {
        "MONA_CD": "congressmanId",
        "HG_NM": "congressmanName",
        "CMIT_NM": "commits",
        "POLY_NM": "partyName",
        "REELE_GBN_NM": "elected",
        "HOMEPAGE": "homepage",
        "ORIG_NM": "district",
        "UNITS": "assemblyNumber",
        'BTH_DATE': 'congressmanBirth',
        'SEX_GBN_NM': 'sex',
        'E_MAIL' : 'email',
        'ASSEM_ADDR' : 'congressmanOffice',
        'TEL_NO' : 'congressmanTelephone',
        'MEM_TITLE' : 'briefHistory'
    }

    # 데이터프레임 컬럼 이름 변환
    df_lawmakers.rename(columns=column_mapping, inplace=True)

    load_dotenv()

    if mode == 'update':
        # 데이터 전송
        payload_name = os.getenv("PAYLOAD_lawmakers")
        url = os.getenv("POST_URL_lawmakers")

        response = send_data(df_lawmakers, url, payload_name)
        
        print("[정당별 의원수 갱신 요청 중...]")
        post_url_party_bill_count = os.environ.get("POST_URL_party_bill_count")
        request_post(post_url_party_bill_count)
        print("[정당별 의원수 갱신 요청 완료]")
    
    elif mode == 'local':
        payload_name = os.getenv("PAYLOAD_lawmakers")
        url = os.getenv("POST_URL_lawmakers")
        url = url.replace("https://api.lawdigest.net", "http://localhost:8080")

        response = send_data(df_lawmakers, url, payload_name)
    
    elif mode == 'test':
        print("[테스트 모드 : DB에 데이터를 전송하지 않습니다.]")
    
    elif mode == 'save': 
        df_lawmakers.to_csv('df_lawmakers.csv', index=False)
        
    else:
        print("모드를 선택해주세요. update | local | test | save")
        
    return df_lawmakers

def fetch_bills_timeline(start_date_str=None, end_date_str=None, age=None):
    load_dotenv()
    
    api_key = os.getenv("APIKEY_status")
    
    all_data = []
    pageNo = 1
    processing_count = 0
    start_time = time.time()

    if start_date_str is None:
        # 어제 날짜를 기본값으로 설정
        start_date_str = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
    if end_date_str is None:
        # 오늘 날짜를 기본값으로 설정
        end_date_str = datetime.now().strftime('%Y-%m-%d')
        
    if age is None:
        age = os.getenv("AGE")

    # 문자열을 datetime 객체로 변환
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    date_range = (end_date - start_date).days + 1
    print(f"\n[{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 의정활동 데이터 수집]")

    max_retry=3
    
    url="https://open.assembly.go.kr/portal/openapi/nqfvrbsdafrmuzixe"

    for single_date in (start_date + timedelta(n) for n in range(date_range)):
        date_str = single_date.strftime('%Y-%m-%d')
        
        while True:
            params = {
                "Key": api_key,
                "Type": "xml",
                "pIndex": pageNo,
                "pSize": 100,
                "AGE": age,
                "DT": date_str
            }
            # print(f"Requesting data for {date_str}, page {pageNo}...")
            
            # API 요청
            response = requests.get(url, params=params, timeout=10)
            
            # 응답 데이터 확인
            if response.status_code == 200:
                try:
                    root = ElementTree.fromstring(response.content)
                    items = root.findall(".//row")
                    if not items:
                        # print(f"No more data available for {date_str}.")
                        break
                    data = []
                    for item in items:
                        row = {}
                        for child in item:
                            row[child.tag] = child.text
                        data.append(row)
                    all_data.extend(data)
                    print(f"Data for {date_str}, page {pageNo} processed. {len(data)} items added. total: {len(all_data)}")
                    processing_count += 1
                except Exception as e:
                    print(f"Error processing response: {str(e)}")
                    max_retry -= 1
            else:
                print(f"Error Code: {response.status_code} (Date: {date_str}, Page {pageNo})")
                max_retry -= 1
            
            if max_retry <= 0:
                print("Maximum retry reached. Exiting...")
                break
            
            if processing_count >= 10:
                processing_count = 0
            
            pageNo += 1
            # time.sleep(1)
        
        pageNo = 1  # 다음 날짜로 넘어갈 때 페이지 번호 초기화

    # 데이터프레임 생성
    df_timeline = pd.DataFrame(all_data)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초")
    print(f"{len(df_timeline)} 개의 의정활동 데이터가 수집됨.")

    return df_timeline


def update_bills_timeline(start_date=None, end_date=None, mode='test', age=None):
    
    if start_date == None:
        load_dotenv()

        host = os.getenv("host")
        port = int(os.getenv("port"))
        username = os.getenv("username")
        password = os.getenv("password")
        database = os.getenv("database")

        conn, cursor = connect_RDS(host, port, username, password, database)

        if conn is not None and cursor is not None:
            latest_timeline_date = get_latest_timeline_date(cursor).strftime('%Y-%m-%d')
            if latest_timeline_date:
                print("The latest propose_date is:", latest_timeline_date)
            cursor.close()
            conn.close()
            
        start_date = latest_timeline_date
    
    
    if end_date == None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    df_stage = fetch_bills_timeline(start_date, end_date, age)
    df_stage = df_stage[['DT', 'BILL_ID' ,'STAGE', 'COMMITTEE']]

    column_mapping = {
        'DT': 'statusUpdateDate',
        'BILL_ID': 'billId',
        'STAGE': 'stage',
        'COMMITTEE': 'committee',
        # 'ACT_STATUS': 'actStatusValue' # 24-12-22 해당 컬럼 dto에서 삭제
    }

    df_stage.rename(columns=column_mapping, inplace=True)
    
    # 데이터 개수 출력
    print("데이터 개수 : ", len(df_stage))

    load_dotenv()
    url = os.getenv("POST_URL_status")
    payloadName = os.getenv("PAYLOAD_status")

    if mode == 'remote':
        total_rows = len(df_stage)
        chunks = [df_stage[i:i+1000] for i in range(0, total_rows, 1000)]
        total_chunks = len(chunks)
        successful_chunks = 0
        failed_chunks = 0
        not_found_bill_count = 0
        
        for i, chunk in enumerate(chunks, 1):
            print(f"[청크 {i}/{total_chunks} 처리 중 (진행률: {i/total_chunks*100:.2f}%)]")
            try:
                response = send_data(chunk, url, payloadName)
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
        url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
        print(f'[로컬 모드 : {url}로 데이터 전송]')
        send_data(df_stage, url, payloadName)

    elif mode == 'test':
        print("[테스트 모드 : 데이터 전송 생략]")
    
    elif mode == 'save':
        df_stage.to_csv("bills_status.csv", index=False)
        print("[데이터 저장 완료]")

    else:
        print("올바른 모드를 선택해주세요. update | local | test | save")
        
    return df_stage


# def fetch_bills_result(start_date_str=None, end_date_str=None):
#     load_dotenv()

#     api_key = os.getenv("APIKEY_status")
#     age = os.getenv("AGE")
#     url = 'https://open.assembly.go.kr/portal/openapi/nwbpacrgavhjryiph'
    

#     all_data = []
#     pageNo = 1
#     processing_count = 0
#     start_time = time.time()

#     # 기본값 설정: start_date는 어제, end_date는 오늘
#     if start_date_str is None:
#         start_date_str = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
#     if end_date_str is None:
#         end_date_str = datetime.now().strftime('%Y-%m-%d')

#     # 문자열을 datetime 객체로 변환
#     start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
#     end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
#     date_range = (end_date - start_date).days + 1

#     print(f"\n[{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 데이터 수집]")

#     for single_date in (start_date + timedelta(n) for n in range(date_range)):
#         date_str = single_date.strftime('%Y-%m-%d')

#         while True:
#             params = {
#                 'KEY': api_key,
#                 'Type': 'xml',
#                 'pIndex': pageNo,
#                 'pSize': 100,
#                 'AGE': age,
#                 'RGS_PROC_DT': date_str  # 본회의심의_의결일에 해당하는 날짜 필터링
#             }
            
#             # print(f"Requesting data for {date_str}, page {pageNo}...")
            
#             # API 요청
#             response = requests.get(url, params=params)
            
#             # 응답 데이터 확인
#             if response.status_code == 200:
#                 try:
#                     root = ElementTree.fromstring(response.content)
#                     head = root.find('head')
#                     if head is None:
#                         # print(f"Error: 'head' element not found in response (Page {pageNo})")
#                         break
                    
#                     total_count_elem = head.find('list_total_count')
#                     if total_count_elem is None:
#                         # print(f"Error: 'list_total_count' element not found in 'head' (Page {pageNo})")
#                         break
                    
#                     total_count = int(total_count_elem.text)
                    
#                     rows = root.findall('row')
#                     if not rows:
#                         print("No more data available.")
#                         break
                    
#                     data = []
#                     for row_elem in rows:
#                         row = {}
#                         for child in row_elem:
#                             row[child.tag] = child.text
#                         data.append(row)
                    
#                     all_data.extend(data)
#                     print(f"date: {date_str} | Page: {pageNo} | total: {len(all_data)}")
#                     processing_count += 1
                    
#                     if pageNo * 100 >= total_count:
#                         # print("All pages processed.")
#                         break
                    
#                 except Exception as e:
#                     print(f"Error: {e}")
#                     break
#             else:
#                 print(f"Error Code: {response.status_code} (Page {pageNo})")
#                 break
            
#             pageNo += 1

#         pageNo = 1  # 다음 날짜로 넘어갈 때 페이지 번호 초기화

#     # 데이터프레임 생성
#     df_vote = pd.DataFrame(all_data)

#     end_time = time.time()
#     total_time = end_time - start_time
#     print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
#     print(f"[{len(df_vote)} 개의 데이터 수집됨]")

#     return df_vote

def fetch_bills_result(start_date=None, end_date=None, age=None):
    
    load_dotenv()
    
    if start_date is None:
        start_date = datetime.now()
    else:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')

    if end_date is None:
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    if age is None:
        age = os.getenv("AGE")

    api_key = os.getenv("APIKEY_result")
    url = 'https://open.assembly.go.kr/portal/openapi/TVBPMBILL11'

    all_data = []
    processing_count = 0
    max_retry = 10

    # 시작과 끝 날짜 출력
    print(f"\n[{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 데이터 수집]")

    start_time = time.time()

    # start_date부터 end_date까지 하루씩 증가하며 데이터 수집
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
                'PROC_DT': current_date.strftime('%Y-%m-%d')  # 현재 날짜를 PROC_DT로 설정
            }

            # print(f"Requesting data for {current_date.strftime('%Y-%m-%d')} (Page {pageNo})...")

            # API 요청
            response = requests.get(url, params=params)

            # 응답 데이터 확인
            if response.status_code == 200:
                try:
                    root = ElementTree.fromstring(response.content)
                    head = root.find('head')
                    if head is None:
                        # print(f"Error: 'head' element not found in response (Page {pageNo})")
                        break

                    total_count_elem = head.find('list_total_count')
                    if total_count_elem is None:
                        # print(f"Error: 'list_total_count' element not found in 'head' (Page {pageNo})")
                        break

                    total_count = int(total_count_elem.text)

                    rows = root.findall('row')
                    if not rows:
                        # print("No more data available for this date.")
                        break

                    data = []
                    for row_elem in rows:
                        row = {}
                        for child in row_elem:
                            row[child.tag] = child.text
                        data.append(row)

                    all_data.extend(data)
                    print(f"{current_date.strftime('%Y-%m-%d')} | page {pageNo} | total: {len(all_data)}")
                    processing_count += 1

                    if pageNo * 100 >= total_count:
                        # print(f"All pages processed for {current_date.strftime('%Y-%m-%d')}.")
                        break

                except Exception as e:
                    print(f"Error: {e}")
                    max_retry -= 1
            else:
                print(f"Error Code: {response.status_code} (Page {pageNo})")
                max_retry -= 1

            if max_retry <= 0:
                print("Maximum retry reached. Exiting...")
                break

            pageNo += 1

        # 다음 날짜로 이동
        current_date += timedelta(days=1)

    # 데이터프레임 생성
    df_result = pd.DataFrame(all_data)
    
    if len(df_result) == 0:
        print("수집된 데이터가 없습니다.")
        return None
    
    # columns_to_keep = [
    # 'BILL_ID', # 의안 ID
    # # 'AGE', # 대수
    # 'PROC_DT', # 의결일
    # 'PROC_RESULT_CD', # 본회의심의결과
    # ]

    # df_result = df_result[columns_to_keep]

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
    print(f"Total dates processed: {(end_date - start_date).days + 1}")
    print(f"[{len(df_result)} 개의 데이터 수집됨]")

    pd.set_option('display.max_columns', None)

    return df_result


def update_bills_result(start_date=None, end_date=None, mode='remote', age=None):
    if start_date is None:
        # 어제 날짜를 기본값으로 설정
        start_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    df_result = fetch_bills_result(start_date, end_date, age)
    
    if df_result is None:
        print("수집된 데이터가 없습니다. 코드를 종료합니다.")
        return None
    
    df_result = df_result[['BILL_ID', 'PROC_RESULT_CD']]
    
    columns_mapping = {
        'BILL_ID': 'billId',
        'PROC_RESULT_CD': 'billProposeResult'
    }
    
    df_result.rename(columns=columns_mapping, inplace=True)
    
    if mode == 'remote': 
        load_dotenv()
        url = os.getenv("POST_URL_result")
        payloadName = os.getenv("PAYLOAD_result")
        
        # 1000개 단위로 데이터 전송
        total_chunks = len(df_result) // 1000 + (1 if len(df_result) % 1000 > 0 else 0)
        for i in range(0, len(df_result), 1000):
            df_chunk = df_result.iloc[i:i+1000]
            response = send_data(df_chunk, url, payloadName)
            current_chunk = i // 1000 + 1
            print(f"진행률: {current_chunk}/{total_chunks} ({current_chunk/total_chunks*100:.2f}%)")
    
    if mode == 'fetch':
        print("[수집 모드 : 데이터 전송 생략]")
    
    if mode == 'local':
        url = os.getenv("POST_URL_result")
        url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
        payloadName = os.getenv("PAYLOAD_result")
        print(f'[로컬 모드 : {url}로 데이터 전송]')
        send_data(df_result, url, payloadName)
    
    return df_result


def fetch_bills_vote(start_date=None, end_date=None, age=None):
    load_dotenv()

    api_key = os.getenv("APIKEY_status")
    
    if age is None:
        age = os.getenv("AGE")
        
    url = 'https://open.assembly.go.kr/portal/openapi/nwbpacrgavhjryiph'
    # url = 'https://open.assembly.go.kr/portal/openapi/TVBPMBILL11'
    

    all_data = []
    pageNo = 1
    processing_count = 0
    start_time = time.time()

    # 기본값 설정: start_date는 어제, end_date는 오늘
    if start_date is None:
        start_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # 문자열을 datetime 객체로 변환
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    date_range = (end_date - start_date).days + 1

    print(f"\n[{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 데이터 수집]")

    for single_date in (start_date + timedelta(n) for n in range(date_range)):
        date_str = single_date.strftime('%Y-%m-%d')

        while True:
            params = {
                'KEY': api_key,
                'Type': 'xml',
                'pIndex': pageNo,
                'pSize': 100,
                'AGE': age,
                'RGS_PROC_DT': date_str  # 본회의심의_의결일에 해당하는 날짜 필터링
                # 'PROC_DT': date_str  # 본회의심의_의결일에 해당하는 날짜 필터링
            }
            
            # print(f"Requesting data for {date_str}, page {pageNo}...")
            
            # API 요청
            response = requests.get(url, params=params)
            
            # 응답 데이터 확인
            if response.status_code == 200:
                try:
                    root = ElementTree.fromstring(response.content)
                    head = root.find('head')
                    if head is None:
                        # print(f"Error: 'head' element not found in response (Page {pageNo})")
                        break
                    
                    total_count_elem = head.find('list_total_count')
                    if total_count_elem is None:
                        # print(f"Error: 'list_total_count' element not found in 'head' (Page {pageNo})")
                        break
                    
                    total_count = int(total_count_elem.text)
                    
                    rows = root.findall('row')
                    if not rows:
                        print("No more data available.")
                        break
                    
                    data = []
                    for row_elem in rows:
                        row = {}
                        for child in row_elem:
                            row[child.tag] = child.text
                        data.append(row)
                    
                    all_data.extend(data)
                    print(f"date: {date_str} | Page: {pageNo} | total: {len(all_data)}")
                    processing_count += 1
                    
                    if pageNo * 100 >= total_count:
                        # print("All pages processed.")
                        break
                    
                except Exception as e:
                    print(f"Error: {e}")
                    break
            else:
                print(f"Error Code: {response.status_code} (Page {pageNo})")
                break
            
            pageNo += 1

        pageNo = 1  # 다음 날짜로 넘어갈 때 페이지 번호 초기화

    # 데이터프레임 생성
    df_vote = pd.DataFrame(all_data)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
    print(f"[{len(df_vote)} 개의 데이터 수집됨]")

    return df_vote

def fetch_vote_party(df_vote, age=None):

    load_dotenv()

    if age is None:
        age = os.getenv("AGE")

    api_key = '946323ab4a694ab580186ad13e821de5'
    url = 'https://open.assembly.go.kr/portal/openapi/nojepdqqaweusdfbi'

    all_data = []
    count = 0
    pageNo = 1
    processing_count = 0
    max_retry = 10

    start_time = time.time()
    count = 0

    for bill_id in df_vote[df_vote['PROC_RESULT_CD'] != '철회']['BILL_ID']:
        pageNo = 1
        while True:
            print(f"Processing data for bill ID: {bill_id}")
            params = {
                'KEY': api_key,
                'Type': 'xml',
                'pIndex': pageNo,
                'pSize': 100,
                'AGE': age,
                'BILL_ID':bill_id,
            }
            
            count += 1
            print(f"Requesting page {pageNo}...")
            
            # API 요청
            response = requests.get(url, params=params)
            
            # 응답 데이터 확인
            if response.status_code == 200:
                try:
                    # 응답 출력 추가
                    # print(response.content.decode('utf-8'))
                    
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
                        row = {}
                        for child in row_elem:
                            row[child.tag] = child.text
                        data.append(row)
                    
                    all_data.extend(data)
                    print(f"Page {pageNo} processed. {len(data)} items added. total: {len(all_data)}")
                    processing_count += 1
                    
                    if pageNo * 100 >= total_count:
                        print("All pages processed.")
                        break
                    
                except Exception as e:
                    print(f"Error: {e}")
                    max_retry -= 1
            else:
                print(f"Error Code: {response.status_code} (Page {pageNo})")
                max_retry -= 1
            
            if max_retry <= 0:
                print("Maximum retry reached. Exiting...")
                break
            
            if processing_count >= 10:
                processing_count = 0

            pageNo += 1

    # 데이터프레임 생성
    df_vote_individual = pd.DataFrame(all_data)
    
    if len(df_vote_individual) == 0:
        print("수집된 데이터가 없습니다.")
        return None

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
    print(f"Total pages processed: {count}")
    print(f"[{len(df_vote_individual)} 개의 데이터 수집됨]")

    df_vote_individual

    columns_to_keep = [
        # 'VOTE_DATE', # 의결일자
        'AGE', # 대수
        'BILL_ID', # 의안번호
        # 'BILL_NAME', # 의안명
        'HG_NM', # 의원명
        # 'HJ_NM', # 의원한자명
        'POLY_NM', # 소속정당
        'RESULT_VOTE_MOD', # 표결결과
    ]

    df_vote_individual = df_vote_individual[columns_to_keep]

    df_vote_party = df_vote_individual[df_vote_individual['RESULT_VOTE_MOD'] == '찬성'].groupby(['BILL_ID', 'POLY_NM']).size().reset_index(name='voteForCount')

    # 컬럼 이름 변경
    column_mapping = {
        'BILL_ID': 'billId',
        'POLY_NM': 'partyName',
        'voteForCount': 'voteForCount'
    }
    df_vote_party.rename(columns=column_mapping, inplace=True)
    
    return df_vote_party

def update_bills_vote(start_date=None, end_date=None, mode='test', age=None):
    
    # 모드 확인 구문 추가 - 250217
    mode_list = ['remote', 'local', 'fetch']

    if mode not in mode_list:
        raise ValueError(f"[올바른 모드를 선택해주세요. 모드 목록: {mode_list}]")

    print(f"[선택한 모드:{mode}]")

    if start_date is None:
        # 어제 날짜를 기본값으로 설정
        start_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    df_vote = fetch_bills_vote(start_date, end_date, age)
    
    if len(df_vote) == 0:
        print("본회의 표결 결과 데이터 없음. 코드를 종료합니다.")
        return None
    
    df_vote_party = fetch_vote_party(df_vote, age)
    
    columns_to_keep = [
        # 'AGE', # 대수
        'BILL_ID', # 의안 ID
        # 'BILL_NO', # 의안번호
        # 'BILL_NM', # 의안명
        # 'RGS_PROC_DT', # 본회의심의_의결일
        # 'PROC_RESULT_CD', # 의결 결과
        'VOTE_TCNT', # 총투표수
        'YES_TCNT', # 찬성
        'NO_TCNT', # 반대
        'BLANK_TCNT' # 기권
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
    
    # 들어온 값이 없을 경우 종료
    if df_vote is None:
        print("본회의 표결 결과 데이터 없음. 코드를 종료합니다.")
        return None
    
    else:
    
        print("[본회의 표결 결과 데이터 전송 시작...]")
        
        url = os.getenv("POST_URL_vote")
        payloadName = os.getenv("PAYLOAD_vote")
        
        if mode == 'remote':
            # 1000개 단위로 데이터 전송
            total_chunks = len(df_vote) // 1000 + (1 if len(df_vote) % 1000 > 0 else 0)
            for i in range(0, len(df_vote), 1000):
                df_chunk = df_vote.iloc[i:i+1000]
                send_data(df_chunk, url, payloadName)
                current_chunk = i // 1000 + 1
                print(f"진행률: {current_chunk}/{total_chunks} ({current_chunk/total_chunks*100:.2f}%)")
            
        if mode == 'local':
            url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
            send_data(df_vote, url, payloadName)
        
        if mode == 'fetch':
            print("[데이터 수집 모드 : 표결 수 데이터 전송 생략]")
    
    if df_vote_party is None:
        print("정당별 표결 결과 데이터가 없습니다. 코드를 종료합니다.")
        return None

    else:

        print("[본회의 정당별 표결 데이터 전송 시작...]")
        
        url = os.getenv("POST_URL_vote_party")
        payloadName = os.getenv("PAYLOAD_vote_party")
        
        if mode == 'remote':
            # 1000개 단위로 데이터 전송
            total_chunks = len(df_vote_party) // 1000 + (1 if len(df_vote_party) % 1000 > 0 else 0)
            for i in range(0, len(df_vote_party), 1000):
                df_chunk = df_vote_party.iloc[i:i+1000]
                send_data(df_chunk, url, payloadName)
                current_chunk = i // 1000 + 1
                print(f"진행률: {current_chunk}/{total_chunks} ({current_chunk/total_chunks*100:.2f}%)")
                
            if mode == 'local':
                url = url.replace("https://api.lawdigest.net", "http://localhost:8080")
                send_data(df_vote_party, url, payloadName)
            
            if mode == 'fetch':
                print("[데이터 수집 모드 : 표결 수 데이터 전송 생략]")
    
    return df_vote, df_vote_party
    

import requests
import xml.etree.ElementTree as ElementTree
import pandas as pd

def fetch_bills_alternatives(df_bills):
    """
    입력받은 법안 데이터프레임을 기반으로 각 법안의 대안을 수집하고 반환하는 함수.
    
    Parameters:
    df_bills (pd.DataFrame): 법안 데이터프레임 (billId와 billName 열 포함)
    
    Returns:
    pd.DataFrame: 각 법안의 대안을 포함하는 데이터프레임
    """
    def fetch_alternativeBills_relation_data(bill_id):
        # API 요청 URL 및 파라미터 설정
        url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillAdditionalInfo'
        params = {
            'serviceKey': 'UJY+e286zOQsAHMHd/5cggpYFaFqG5mWawJKgrubJeKRBqVp0VUsyeHIgw/VGPQjWRSp6yaR/sUhXlhpKyv1cg==',
            'bill_id': bill_id
        }
        
        try:
            # API 요청
            response = requests.get(url, params=params)
            if response.status_code == 200:
                # 응답 데이터 파싱
                root = ElementTree.fromstring(response.content)
                items = root.find('.//exhaust')
                
                if items is None or len(items.findall('item')) == 0:
                    return []
                
                # 법률안 데이터 추출
                law_data = []
                for item in items.findall('item'):
                    bill_link = item.find('billLink').text
                    law_bill_id = bill_link.split('bill_id=')[-1]
                    bill_name = item.find('billName').text.encode('utf-8').decode('utf-8')  # 한글 디코딩
                    law_data.append({'billId': law_bill_id, 'billName': bill_name})
                return law_data
            else:
                print(f"Error for bill_id={bill_id}, status_code={response.status_code}")
                return []
        except Exception as e:
            print(f"Error for bill_id={bill_id}: {e}")
            return []
    
    # 대안 데이터프레임 초기화
    alternatives_data = []

    # 각 법안의 billId를 이용해 대안 데이터 수집
    for _, row in tqdm(df_bills.iterrows(), total=len(df_bills)):
        alt_id = row['billId']  # 대안(위원장안) ID
        # bill_name = row['billName']  # 이름
        
        # 대안 데이터 수집
        law_data = fetch_alternativeBills_relation_data(alt_id)
        
        # 수집된 데이터를 정리하여 리스트에 추가
        for law in law_data:
            alternatives_data.append({
                'altBillId': alt_id,  # 대안(위원장안) ID
                # 'originalBillName': bill_name, 
                'billId': law['billId'],  # 대안에 포함된 법안 ID
                # 'altBillName': law['billName'] 
            })
    
    # 대안 데이터를 데이터프레임으로 변환
    df_alternatives = pd.DataFrame(alternatives_data)
    return df_alternatives


def update_bills_alternatives(df_alternatives=None, fetch_mode=None, update_mode=None):
    fetch_mode_list = ['daily', 'total']
    
    if fetch_mode not in fetch_mode_list:
        print("올바른 모드를 입력해주세요. 모드 목록: ", fetch_mode_list)
        return 0
    
    if fetch_mode == 'total':
        load_dotenv()
        api_key = os.environ.get("APIKEY_billsContent")
        url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'
        params = {
            'serviceKey': api_key,
            'numOfRows': '100',
            'start_ord': '22',
            'end_ord': '22',
            # 'start_propose_date': start_date,
            # 'end_propose_date': end_date,
            'proposer_kind_cd': 'F02'
        }

        # 데이터 수집 시작
        all_data = []
        pageNo = 1
        processing_count = 0
        max_retry=3

        start_time = time.time()

        while True:
            params.update({'pageNo': str(pageNo)})
            response = requests.get(url, params=params)
            
            print(response.content)
            
            if response.status_code == 200:
                try:
                    root = ElementTree.fromstring(response.content)
                    items = root.find('body').find('items')
                    
                    if items is None or len(items) == 0:
                        # print("No more data available.")
                        break
                    
                    data = [{child.tag: child.text for child in item} for item in items]
                    all_data.extend(data)
                    
                    # print(f"Page {pageNo} processed. {len(data)} items added. total: {len(all_data)}")
                    processing_count += 1
                    
                except ElementTree.ParseError:
                    print(f"XML Parsing Error: {response.text}")
                    max_retry -= 1
                except Exception as e:
                    print(f"Unexpected Error: {e}")
                    max_retry -= 1
            else:
                print(f"Error Code: {response.status_code} (Page {pageNo})")
                max_retry -= 1
            
            if max_retry <= 0:
                print("Maximum retry reached. Exiting...")
                break
            
            pageNo += 1

        # 데이터프레임 생성
        df_billsContent = pd.DataFrame(all_data)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"[모든 파일 다운로드 완료! 전체 소요 시간: {total_time:.2f}초]")
        print(f"[{len(df_billsContent)} 개의 법안 수집됨.]")

        # 수집한 데이터가 없으면 종료
        if len(df_billsContent) == 0:
            print("수집한 데이터가 없습니다. 코드를 종료합니다.")
            # return None

        # 유지할 컬럼 목록
        columns_to_keep = [
            'proposeDt', # 발의일자
            'billId', # 법안id
            # 'billName', # 법안명
            # 'summary', # 주요내용
            # 'procStageCd', # 현재 처리 단계
            # 'generalResult' # 처리 결과
            'proposerKind' # 발의자 종류(전부 '위원장' 이어야 함)
        ]

        # 지정된 컬럼만 유지하고 나머지 제거
        df_billsContent = df_billsContent[columns_to_keep]

        print(f"[결측치 처리 완료. {len(df_billsContent)} 개의 법안 수집됨.]")
        print("\n발의일자별 수집한 데이터 수 :")
        print(f"{df_billsContent['proposeDt'].value_counts()}")    
        
        df_alt_ids = df_billsContent
        
        df_bills_alternatives = fetch_bills_alternatives(df_alt_ids)
        
        print("[22대 전체 대안-법안 관계 데이터 수집 완료됨]")
        
        print(df_bills_alternatives.info())
        
        return df_bills_alternatives
        
        
                
                
