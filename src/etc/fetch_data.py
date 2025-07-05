import requests
import pandas as pd
from xml.etree import ElementTree
import json
import os # os 모듈 추가
from tqdm import tqdm # tqdm 라이브러리 import

# --- 범용 함수 (수정 없음) ---
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
        
        # tqdm 사용 시 print는 tqdm.write로 감싸주는 것이 좋지만, 간단한 정보 표시는 그대로 둬도 무방합니다.
        # print(f"   [API 응답] 코드: {result_code}, 메시지: {result_msg}")
        if result_code != mapper['success_code']: return [], 0
        return data, total_count
    except Exception as e:
        print(f"   ❌ 응답 파싱 중 오류 발생: {e}")
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

    # all_pages=False이면 여기서 수집한 첫 페이지만 반환하고 종료
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
                
                # 두 번째 페이지부터는 total_count 값이 필요 없으므로 _로 받습니다.
                data, _ = _parse_response(response.content, format, mapper)
                
                if not data:
                    pbar.set_description("⚠️ API 응답에 더 이상 데이터가 없습니다")
                    break
                
                all_data.extend(data)
                pbar.update(len(data)) # 새로 가져온 데이터 개수만큼 진행률 바를 업데이트
                retries_left = max_retry

            except Exception as e:
                # tqdm 진행률 바와 충돌하지 않게 오류 메시지를 출력합니다.
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
    # 1. API에 맞는 '작업 설명서(mapper)' 만들기
    bills_xml_mapper = {
        "page_param": "pageNo",
        "size_param": "numOfRows",
        "data_path": ".//item",
        "total_count_path": ".//totalCount",
        "result_code_path": ".//resultCode",
        "result_msg_path": ".//resultMsg",
        "success_code": "00"
    }

    # 2. 함수 호출에 필요한 정보 준비
    bills_api_url = 'http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList'
    bills_api_params = {
        "serviceKey": os.environ.get("APIKEY_billsContent"),
        "pageNo": 1,
        "numOfRows": 100, # 한 번에 가져올 데이터 개수를 늘리면 속도가 빨라집니다.
        'start_ord': os.environ.get("AGE"),
        'end_ord': os.environ.get("AGE"),
        'start_propose_date': '2025-06-01',
        'end_propose_date': '2025-07-05'
    }

    print("--- 공공데이터포털 의안정보 API 수집 테스트 ---")
    # 3. 함수 호출!
    df_result = fetch_data_generic(
        url=bills_api_url,
        params=bills_api_params,
        mapper=bills_xml_mapper,
        verbose=False,
        format='xml'
    )
    
    if not df_result.empty:
        print(df_result.head())
