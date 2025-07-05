import requests
import pandas as pd
from xml.etree import ElementTree
import json
import os
from tqdm import tqdm

# --- 범용 함수 (이전과 동일, 수정 없음) ---
def _get_nested_value(data, path):
    current_level = data
    for key in path:
        if isinstance(current_level, dict): current_level = current_level.get(key)
        elif isinstance(current_level, list) and isinstance(key, int):
            try: current_level = current_level[key]
            except IndexError: return None
        else: return None
        if current_level is None: return None
    return current_level

def _parse_response(response_content, format, mapper):
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
            data = _get_nested_value(response_json, mapper['data_path']) or []
            total_count = int(_get_nested_value(response_json, mapper['total_count_path']))
            result_code = _get_nested_value(response_json, mapper['result_code_path'])
            result_msg = _get_nested_value(response_json, mapper['result_msg_path'])
        
        if result_code != mapper['success_code']:
            # tqdm 진행률 바와 충돌하지 않게 오류 메시지를 출력합니다.
            tqdm.write(f"   [API 응답 실패] 코드: {result_code}, 메시지: {result_msg}")
            return [], 0
        return data, total_count
    except Exception as e:
        tqdm.write(f"   ❌ 응답 파싱 중 오류 발생: {e}")
        return [], 0

def fetch_data_generic(url, params, mapper, format='json', all_pages=True, verbose=False, max_retry=3):
    page_param = mapper.get('page_param')
    if all_pages and not page_param:
        raise ValueError("'all_pages=True'일 경우, 매퍼에 'page_param'이 정의되어야 합니다.")
    
    all_data = []
    current_params = params.copy()

    # 1. 첫 페이지를 먼저 요청하여 total_count를 얻습니다.
    print("➡️  첫 페이지 요청하여 전체 데이터 개수 확인 중...")
    try:
        response = requests.get(url, params=current_params)
        response.raise_for_status()
        if verbose: print(response.content.decode('utf-8'))
        
        initial_data, total_count = _parse_response(response.content, format, mapper)
        
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

    # 2. tqdm 프로그레스 바로 나머지 페이지를 처리합니다.
    with tqdm(total=total_count, initial=len(all_data), desc="📥 데이터 수집 중", unit="개") as pbar:
        retries_left = max_retry
        
        while len(all_data) < total_count:
            current_params[page_param] += 1
            
            try:
                response = requests.get(url, params=current_params)
                response.raise_for_status()
                data, _ = _parse_response(response.content, format, mapper)
                
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

# --- 여기가 실제 사용법입니다 ---
if __name__ == '__main__':
    # =================================================================
    # 예시 1: 열린국회정보 API (신규 작성)
    # =================================================================
    print("--- 열린국회정보 API 수집 테스트 ---")
    
    # 1. '열린국회정보' API를 위한 mapper 작성
    openassembly_xml_mapper = {
        "page_param": "pIndex",
        "size_param": "pSize",
        "data_path": ".//row",                  # 데이터 항목 경로
        "total_count_path": ".//list_total_count", # 전체 개수 경로
        "result_code_path": ".//RESULT/CODE",      # 결과 코드 경로
        "result_msg_path": ".//RESULT/MESSAGE",     # 결과 메시지 경로
        "success_code": "INFO-000"              # 성공 코드
    }

    # 2. API URL 및 파라미터 준비
    openassembly_api_url = 'https://open.assembly.go.kr/portal/openapi/VCONFBILLLIST' #open.assembly.go.kr로 시작하는 url은 열린국회정보 api
    openassembly_api_params = {
        "KEY": "YOUR_ASSEMBLY_API_KEY", # 실제 발급받은 키로 교체 필요
        "Type": "xml",
        "pIndex": 1,
        "pSize": 100, # 한 번에 가져올 데이터 개수를 늘리면 속도가 빨라집니다.
    }

    # 3. 함수 호출!
    # 아래 주석을 해제하고 유효한 KEY를 입력하면 실제 동작을 테스트할 수 있습니다.
    # df_assembly = fetch_data_generic(
    #     url=openassembly_api_url,
    #     params=openassembly_api_params,
    #     mapper=openassembly_xml_mapper,
    #     format='xml'
    # )
    
    # if not df_assembly.empty:
    #     print(df_assembly.head())

    print("\n" + "="*50 + "\n")

    # =================================================================
    # 예시 2: 공공데이터포털 API (기존)
    # =================================================================
    print("--- 공공데이터포털 의안정보 API 수집 테스트 ---")
    
    datagokr_xml_mapper = {
        "page_param": "pageNo",
        "size_param": "numOfRows",
        "data_path": ".//item",
        "total_count_path": ".//totalCount",
        "result_code_path": ".//resultCode",
        "result_msg_path": ".//resultMsg",
        "success_code": "00"
    }

    datagokr_api_url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList' #apis.data.go.kr로 시작하는 url은 공공데이터포털 api
    datagokr_api_params = {
        "serviceKey": "YOUR_PUBLIC_DATA_API_KEY", # 실제 발급받은 키로 교체 필요
        "pageNo": 1,
        "numOfRows": 100,
    }

    # 아래 주석을 해제하고 유효한 serviceKey를 입력하면 실제 동작을 테스트할 수 있습니다.
    # df_bills = fetch_data_generic(
    #     url=datagokr_api_url,
    #     params=datagokr_api_params,
    #     mapper=datagokr_xml_mapper,
    #     format='xml'
    # )
    
    # if not df_bills.empty:
    #     print(df_bills.head())
    pass
