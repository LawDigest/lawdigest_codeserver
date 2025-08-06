import requests
import pandas as pd
from xml.etree import ElementTree
import time
from datetime import datetime, timedelta
from IPython.display import clear_output
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re
from tqdm import tqdm

import json

class DataFetcher:
    def __init__(self, params, subject=None, url=None, filter_data=True):

        self.params = params  # 요청변수
        self.url = url  # 모드(처리방식)
        self.filter_data = filter_data
        self.content = None  # 수집된 데이터
        self.df_bills = None
        self.df_lawmakers = None
        self.df_vote = None
        self.subject = subject

        # 열린국회정보(json) 매퍼
        self.mapper_open_json = {
            "page_param": "pIndex",
            "size_param": "pSize",
            "data_path": ["ALLBILL", 1, "row"],
            "total_count_path": ["ALLBILL", 0, "head", 0, "list_total_count"],
            "result_code_path": ["ALLBILL", 0, "head", 1, "RESULT", "CODE"],
            "result_msg_path": ["ALLBILL", 0, "head", 1, "RESULT", "MESSAGE"],
            "success_code": "INFO-000",
        }

        # 열린국회정보(xml) 매퍼
        self.mapper_open_xml = {
            "page_param": "pIndex",
            "size_param": "pSize",
            "data_path": ".//row",
            "total_count_path": ".//list_total_count",
            "result_code_path": ".//RESULT/CODE",
            "result_msg_path": ".//RESULT/MESSAGE",
            "success_code": "INFO-000",
        }

        # 공공데이터포털(xml) 매퍼
        self.mapper_datagokr_xml = {
            "page_param": "pageNo",
            "size_param": "numOfRows",
            "data_path": ".//item",
            "total_count_path": ".//totalCount",
            "result_code_path": ".//resultCode",
            "result_msg_path": ".//resultMsg",
            "success_code": "00",
        }

        load_dotenv()

        self.content = self.fetch_data(self.subject)

    def fetch_data(self, subject):
        
        match subject:
            # case "bill_info":
            #     return self.fetch_bills_info()
            case "bills":
                return self.fetch_bills_data()
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

    # ------------------------------------------------------------------
    # Generic API helpers originally from etc/fetch_data_generic.py
    # ------------------------------------------------------------------

    def _get_nested_value(self, data, path):
        current_level = data
        for key in path:
            if isinstance(current_level, dict):
                current_level = current_level.get(key)
            elif isinstance(current_level, list) and isinstance(key, int):
                try:
                    current_level = current_level[key]
                except IndexError:
                    return None
            else:
                return None
            if current_level is None:
                return None
        return current_level

    def _parse_response(self, response_content, format, mapper):
        data, total_count, result_code, result_msg = [], 0, None, "No message"
        try:
            if format == 'xml':
                root = ElementTree.fromstring(response_content)
                data = [{child.tag: child.text for child in item} for item in root.findall(mapper['data_path'])]
                total_count = int(root.find(mapper['total_count_path']).text)
                result_code = root.find(mapper['result_code_path']).text
                result_msg = root.find(mapper['result_msg_path']).text
            elif format == 'json':
                response_json = json.loads(response_content)
                data = self._get_nested_value(response_json, mapper['data_path']) or []
                total_count = int(self._get_nested_value(response_json, mapper['total_count_path']))
                result_code = self._get_nested_value(response_json, mapper['result_code_path'])
                result_msg = self._get_nested_value(response_json, mapper['result_msg_path'])

            if result_code != mapper['success_code']:
                tqdm.write(f"   [API 응답 실패] 코드: {result_code}, 메시지: {result_msg}")
                return [], 0
            return data, total_count
        except Exception as e:
            tqdm.write(f"   ❌ 응답 파싱 중 오류 발생: {e}")
            return [], 0

    def fetch_data_generic(self, url, params, mapper, format='json', all_pages=True, verbose=False, max_retry=3):
        page_param = mapper.get('page_param')
        if all_pages and not page_param:
            raise ValueError("'all_pages=True'일 경우, 매퍼에 'page_param'이 정의되어야 합니다.")

        all_data = []
        current_params = params.copy()

        print("➡️  첫 페이지 요청하여 전체 데이터 개수 확인 중...")
        try:
            response = requests.get(url, params=current_params)
            response.raise_for_status()
            if verbose:
                print(response.content.decode('utf-8'))

            initial_data, total_count = self._parse_response(response.content, format, mapper)

            if total_count == 0 and not initial_data:
                print("⚠️  수집할 데이터가 없거나 API 응답에 문제가 있습니다.")
                return pd.DataFrame()

            all_data.extend(initial_data)

        except Exception as e:
            print(f"❌ 첫 페이지 요청 오류: {e}")
            return pd.DataFrame()

        if not all_pages:
            df = pd.DataFrame(all_data)
            print(f"\n🎉 다운로드 완료! 총 {len(df)}개의 데이터를 수집했습니다. 📊")
            return df

        with tqdm(total=total_count, initial=len(all_data), desc="📥 데이터 수집 중", unit="개") as pbar:
            retries_left = max_retry

            while len(all_data) < total_count:
                current_params[page_param] += 1

                try:
                    response = requests.get(url, params=current_params)
                    response.raise_for_status()
                    data, _ = self._parse_response(response.content, format, mapper)

                    if not data:
                        pbar.set_description("⚠️ API 응답에 더 이상 데이터가 없습니다")
                        break

                    all_data.extend(data)
                    pbar.update(len(data))
                    retries_left = max_retry

                except Exception as e:
                    pbar.write(f"❌ 오류 발생 (페이지 {current_params[page_param]}): {e}")
                    retries_left -= 1
                    if retries_left <= 0:
                        pbar.write("\n🚨 최대 재시도 횟수를 초과했습니다.")
                        break

        df = pd.DataFrame(all_data)
        print(f"\n🎉 다운로드 완료! 총 {len(df)}개의 데이터를 수집했습니다. 📊")
        return df
        
    def fetch_bills_data(self):
        """법안 주요 내용 데이터를 API에서 수집하는 함수."""

        start_date = self.params.get(
            "start_date",
            (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        )
        end_date = self.params.get("end_date", datetime.now().strftime('%Y-%m-%d'))

        api_key = os.environ.get("APIKEY_billsContent")
        url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'
        mapper = self.mapper_datagokr_xml

        params = {
            'serviceKey': api_key,
            mapper['page_param']: 1,
            mapper['size_param']: 100,
            'start_ord': self.params.get("start_ord", os.environ.get("AGE")),
            'end_ord': self.params.get("end_ord", os.environ.get("AGE")),
            'start_propose_date': start_date,
            'end_propose_date': end_date,
        }

        print(f"📌 [{start_date} ~ {end_date}] 의안 주요 내용 데이터 수집 시작...")

        df_bills = self.fetch_data_generic(
            url=url,
            params=params,
            mapper=mapper,
            format='xml',
            all_pages=True,
        )

        if df_bills.empty:
            raise AssertionError(
                "❌ [ERROR] 수집된 데이터가 없습니다. API 응답을 확인하세요."
            )

        print(f"✅ [INFO] 총 {len(df_bills)} 개의 법안 수집됨.")

        if self.filter_data:
            print("✅ [INFO] 데이터 컬럼 필터링을 수행합니다.")
            # 유지할 컬럼 목록
            columns_to_keep = [
                'proposeDt',  # 발의일자
                'billId', # 법안 ID
                'billName', # 법안 이름
                'billNo',  # 법안번호
                'summary',  # 주요내용
                'procStageCd',  # 현재 처리 단계
                'proposerKind' # 발의자 종류
            ]

            # 지정된 컬럼만 유지하고 나머지 제거
            df_bills = df_bills[columns_to_keep]

            # 'summary' 컬럼에 결측치가 있는 행 제거
            df_bills = df_bills.dropna(subset=['summary'])

            # 인덱스 재설정
            df_bills.reset_index(drop=True, inplace=True)

            print(f"✅ [INFO] 결측치 처리 완료. {len(df_bills)} 개의 법안 유지됨.")

        else:
            print("✅ [INFO] 데이터 컬럼 필터링을 수행하지 않습니다.")

        # 컬럼 이름 변경
        df_bills.rename(columns={
            "proposeDt": "proposeDate",
            "billNo": "billNumber",
            "summary": "summary",
            "procStageCd": "stage"
        }, inplace=True)

        # AssemblyNumber는 데이터 호출에 사용된 환경변수 AGE에서 가져오기
        df_bills['assemblyNumber'] = os.environ.get("AGE") 


        print("\n📌 발의일자별 수집한 데이터 수:")
        print(df_bills['proposeDate'].value_counts()) 

        self.content = df_bills
        self.df_bills = df_bills

        return df_bills

    # def fetch_bills_info(self):
    #     """법안 기본 정보를 API에서 가져오는 함수."""

    #     # bill_id가 있는 법안 내용 데이터 수집
    #     if self.df_bills is None:
    #         print("✅ [INFO] 법안정보 수집 대상 bill_no 수집을 위해 법안 내용 API로부터 정보를 수집합니다.")
    #         df_bills = self.fetch_bills_data()
    #     else:
    #         df_bills = self.df_bills

    #     if df_bills is None or df_bills.empty:
    #         print("❌ [ERROR] `df_bills` 데이터가 없습니다. 올바른 값을 전달하세요.")
    #         return None

    #     api_key = os.environ.get("APIKEY_billsInfo")
    #     url = self.url or "https://open.assembly.go.kr/portal/openapi/ALLBILL"

    #     # 출처에 따른 매퍼 설정
    #     if "open.assembly.go.kr" in url:
    #         mapper = self.mapper_open_json
    #         format = "json"
    #     else:
    #         mapper = self.mapper_datagokr_xml
    #         format = "xml"

    #     all_data = []
    #     print(f"\n📌 [법안 정보 데이터 수집 중...]")
    #     start_time = time.time()

    #     for row in tqdm(df_bills.itertuples(), total=len(df_bills)):
    #         params = {
    #             "Key": api_key,
    #             mapper.get("page_param", "pIndex"): 1,
    #             mapper.get("size_param", "pSize"): 5,
    #             "Type": format,
    #             "BILL_NO": row.billNumber,
    #         }

    #         df_tmp = self.fetch_data_generic(
    #             url=url,
    #             params=params,
    #             mapper=mapper,
    #             format=format,
    #             all_pages=True,
    #         )

    #         if not df_tmp.empty:
    #             all_data.extend(df_tmp.to_dict("records"))

    #     df_bills_info = pd.DataFrame(all_data)

    #     end_time = time.time()
    #     total_time = end_time - start_time
    #     print(f"✅ [INFO] 다운로드 완료! 총 소요 시간: {total_time:.2f}초")

    #     if df_bills_info.empty:
    #         print("❌ [ERROR] 수집한 데이터가 없습니다.")
    #         return None

    #     print(f"✅ [INFO] 총 {len(df_bills_info)}개의 법안 정보 데이터가 수집되었습니다.")

    #     if self.filter_data:
    #         print("✅ [INFO] 데이터 컬럼 필터링을 수행합니다.")
    #         columns_to_keep = ['ERACO', 'BILL_ID', 'BILL_NO', 'BILL_NM', 'PPSR_NM', 'JRCMIT_NM']
    #         df_bills_info = df_bills_info[columns_to_keep]

    #         column_mapping = {
    #             'ERACO': 'assemblyNumber',
    #             'BILL_ID': 'billId',
    #             'BILL_NO': 'billNumber',
    #             'BILL_NM': 'billName',
    #             'PPSR_NM': 'proposers',
    #             'JRCMIT_NM': 'committee'
    #         }
    #         df_bills_info.rename(columns=column_mapping, inplace=True)

    #         def extract_names(proposer_str):
    #             return re.findall(r'[가-힣]+(?=의원)', proposer_str) if isinstance(proposer_str, str) else []

    #         df_bills_info['rstProposerNameList'] = df_bills_info['proposers'].apply(extract_names)
    #         df_bills_info['assemblyNumber'] = df_bills_info['assemblyNumber'].str.replace(r'\D', '', regex=True)
    #         print("✅ [INFO] 컬럼 필터링 및 컬럼명 변경 완료.")

    #     self.content = df_bills_info
    #     return df_bills_info

    def fetch_lawmakers_data(self):
        """
        국회의원 데이터를 API로부터 가져와서 DataFrame으로 반환하는 함수.
        API 키와 URL은 함수 내부에서 정의되며, 모든 페이지를 처리합니다.

        Returns:
        - df_lawmakers: pandas.DataFrame, 수집된 국회의원 데이터
        """
        api_key = os.environ.get("APIKEY_lawmakers")
        url = 'https://open.assembly.go.kr/portal/openapi/nwvrqwxyaytdsfvhu'  # 열린국회정보 '국회의원 인적사항' API
        mapper = self.mapper_open_xml

        params = {
            'KEY': api_key,
            'Type': 'xml',
            mapper['page_param']: 1,
            mapper['size_param']: 100,
        }

        print("\n📌 [국회의원 데이터 수집 시작]")
        start_time = time.time()

        df_lawmakers = self.fetch_data_generic(
            url=url,
            params=params,
            mapper=mapper,
            format='xml',
            all_pages=True,
        )

        end_time = time.time()
        total_time = end_time - start_time
        print(f"✅ [INFO] 다운로드 완료! 총 소요 시간: {total_time:.2f}초")

        if df_lawmakers.empty:
            print("❌ [ERROR] 수집한 데이터가 없습니다.")
            return None

        print(f"✅ [INFO] 총 {len(df_lawmakers)} 개의 의원 데이터 수집됨")

        self.content = df_lawmakers
        return df_lawmakers


    def fetch_bills_coactors(self, df_bills=None):
            """
            billId를 사용하여 각 법안의 공동 발의자 명단을 수집하는 함수.
            """

            # `df_bills`가 없으면 `fetch_bills_data()`를 호출하여 자동으로 수집
            if df_bills is None:
                print("✅ [INFO] 법안 공동발의자 명단 정보 수집 대상 bill_no 수집을 위해 법안 내용 API로부터 정보를 수집합니다.")
                df_bills = self.fetch_bills_data()

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
        if df_vote is None:
            print("⚠️ [WARNING] 수집에 필요한 df_vote 데이터가 없습니다. 새로 수집합니다.")
            self.fetch_bills_vote()
            df_vote = self.df_vote  # Update local df_vote

        if df_vote is None or df_vote.empty:
            print("🚨 [WARNING] 해당 날짜에 수집 가능한 데이터가 없습니다. 코드를 종료합니다.")
            return None

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
            df_bills = self.fetch_bills_data()

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

