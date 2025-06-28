import requests
from bs4 import BeautifulSoup, Comment
from datetime import datetime
import html  # HTML 엔티티 디코딩을 위한 모듈
import re  # 정규 표현식 모듈
import time


def clean_html_content(html_content):
    """
    HTML 콘텐츠에서 불필요한 태그와 스타일, MS Word 마크업을 제거하고
    각 항목이 시간 기준으로 독립된 줄에 표시되도록 재구성합니다.
    """
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. script, style 태그 제거
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()

    # 2. 모든 HTML 주석 제거
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # 3. <o:p> 태그와 같은 빈/불필요한 태그 제거
    for op_tag in soup.find_all('o:p'):
        op_tag.decompose()

    # 4. 모든 텍스트 노드를 추출하여 하나의 긴 문자열로 결합 (줄바꿈은 스페이스로 대체)
    # 이렇게 하면 원본 HTML의 모든 텍스트가 한 줄로 연결됩니다.
    raw_text = soup.get_text(separator=' ', strip=True)

    # 5. HTML 엔티티 디코딩 및 MS Word 마크업, &nbsp; 제거
    raw_text = html.unescape(raw_text)
    raw_text = re.sub(r'\[if\s*!supportEmptyParas\]\[endif\]\s*', '', raw_text)
    raw_text = re.sub(r'\s*&nbsp;\s*', '', raw_text)

    # 여러 공백을 단일 공백으로 압축
    raw_text = re.sub(r'\s+', ' ', raw_text).strip()

    # 6. '◇' 카테고리명 패턴 처리: '◇'와 카테고리명 사이의 공백 제거 후, 임시 구분자 삽입
    # 먼저 '◇국회의 장' -> '◇국회의장'처럼 붙입니다.
    raw_text = re.sub(r'(◇)\s*([가-힣]+)', r'\1\2', raw_text)

    # 그 다음, '◇카테고리명' 앞에 임시 구분자(###CATEGORY_START###)를 삽입합니다.
    # 이렇게 하면 문자열의 시작 부분이나 다른 내용 중간에 있더라도 구분자가 삽입됩니다.
    raw_text = re.sub(r'(◇[가-힣]+)', r'###CATEGORY_START###\n\1', raw_text) # 여기에 \n을 추가

    # 7. 'HH:MM' 형태의 시간 앞에 임시 구분자(###TIME_START###) 삽입
    # '07: 30'처럼 시간 중간의 공백도 먼저 제거합니다.
    raw_text = re.sub(r'(\d{2}):\s*(\d{2})', r'\1:\2', raw_text)
    # 이제 'HH:MM' 패턴 앞에 임시 구분자를 삽입합니다.
    # 이미 '###CATEGORY_START###' 뒤에 오는 시간은 예외 처리하지 않고 일단 모두 삽입.
    raw_text = re.sub(r'(\d{2}:\d{2})', r'###TIME_START###\1', raw_text)

    # 8. 최종 텍스트 재구성: 임시 구분자를 줄바꿈으로 치환
    # `raw_text`를 임시 구분자를 기준으로 분리합니다.
    parts = re.split(r'(###CATEGORY_START###|###TIME_START###)', raw_text)

    reconstructed_lines = []

    # 첫 번째 파트가 내용일 수 있으므로 먼저 처리
    if parts and parts[0].strip():
        reconstructed_lines.append(parts[0].strip())

    # 나머지 파트 처리
    for i in range(1, len(parts), 2):
        delimiter = parts[i]
        content = parts[i + 1].strip() if i + 1 < len(parts) else ''

        if not content:  # 내용이 없으면 건너뜁니다.
            continue

        if delimiter == '###CATEGORY_START###':
            # 카테고리는 항상 새 줄에, 그리고 뒤에 줄바꿈 추가
            reconstructed_lines.append(content)  # 카테고리 자체 추가
        elif delimiter == '###TIME_START###':
            # 시간은 항상 새 줄에, 내용과 함께 추가
            reconstructed_lines.append(content)
        else:  # 임시 구분자가 아닌 다른 내용 (오류 방지용)
            if content:
                reconstructed_lines.append(content)

    # 9. 괄호, 특수 기호 주변 공백 정리 (각 줄에 대해)
    final_output_lines = []
    for line in reconstructed_lines:
        line = re.sub(r'\s*([([{])\s*', r'\1', line)
        line = re.sub(r'\s*([)\]}])', r'\1', line)

        line = re.sub(r'(\d+) (년|호)', r'\1\2', line)
        line = re.sub(r'제\s*(\d)\s*소회의실', r'제\1소회의실', line)

        line = re.sub(r'\s*,\s*', ',', line)
        line = re.sub(r'\s*:\s*', ':', line)
        line = re.sub(r'\s*-\s*', '-', line)
        line = re.sub(r'\s*\.\s*', '.', line)

        line = re.sub(r'([.?!])\s*([가-힣A-Za-z0-9])', r'\1\2', line)
        line = re.sub(r':([가-힣A-Za-z0-9])', r': \1', line)

        # 항목 내의 여러 공백을 단일 공백으로
        line = re.sub(r'\s+', ' ', line).strip()
        if line:  # 빈 줄 방지
            final_output_lines.append(line)

    # 10. 최종 줄바꿈 정리
    output_string = "\n".join(final_output_lines)
    output_string = re.sub(r'(\S)\s*(※)', r'\1\n\n\2', output_string) # 이 줄을 추가

    # 중복 줄바꿈을 1개 또는 2개로 제한 (카테고리와 다음 항목 사이에 한 줄 공백 허용)
    output_string = re.sub(r'\n{3,}', '\n\n', output_string)
    output_string = re.sub(r'\n\s*\n', '\n\n', output_string).strip()

    return output_string


def crawl_assembly_and_clean_text():
    """
    대한민국 국회 웹페이지에서 동적으로 로딩되는 '오늘의 국회' 내용을 크롤링하고,
    불필요한 HTML을 제거하여 순수 텍스트만 추출합니다.
    """
    base_url = "https://www.assembly.go.kr"
    main_page_url = f"{base_url}/portal/main/main.do"
    api_url = f"{base_url}/portal/main/nowNaContents/nowNaContents.json"

    session = requests.Session()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Referer": main_page_url,  # 메인 페이지에서 API 요청이 시작되는 것처럼 설정
        "Accept": "application/json, text/javascript, */*; q=0.01",  # JSON 응답을 선호함을 알림
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "X-Requested-With": "XMLHttpRequest",  # AJAX 요청임을 알리는 헤더
        "DNT": "1"  # 추적 방지 (Do Not Track) 헤더 추가 - 선택 사항
    }

    try:
        print("메인 페이지에서 CSRF 토큰 및 날짜 정보 추출 중...")
        response_main = session.get(main_page_url, headers=headers)
        response_main.raise_for_status()  # HTTP 오류 발생 시 예외 발생

        soup_main = BeautifulSoup(response_main.text, 'html.parser')

        # CSRF 토큰 추출
        csrf_meta = soup_main.find('meta', {'name': '_csrf'})
        csrf_token = csrf_meta['content'] if csrf_meta else None

        # 현재 날짜 추출 (HTML 내의 input[id="currFormattedDate"]에서)
        curr_formatted_date_input = soup_main.find('input', {'id': 'currFormattedDate'})
        curr_formatted_date = curr_formatted_date_input['value'] if curr_formatted_date_input else None

        if not csrf_token:
            print("오류: CSRF 토큰을 찾을 수 없습니다. 웹 페이지 구조가 변경되었을 수 있습니다.")
            return None
        if not curr_formatted_date:
            print("오류: 현재 날짜 정보를 찾을 수 없습니다. 웹 페이지 구조가 변경되었을 수 있습니다.")
            return None

        print(f"추출된 CSRF 토큰: {csrf_token}")
        print(f"추출된 현재 날짜: {curr_formatted_date}")

        # 2. 추출한 정보들을 사용하여 JSON API 엔드포인트에 POST 요청 전송
        print("API 엔드포인트에 데이터 요청 중...")
        api_params = {
            "bbsId": "B0000176",
            "optn3": curr_formatted_date,
            "firstIndex": 0,
            "recordCountPerPage": 10,
            "_csrf": csrf_token
        }

        # API 요청을 위한 헤더 설정 (Content-Type이 중요)
        api_headers = headers.copy()  # 기존 헤더 복사
        api_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        # Referer는 이미 headers에 main_page_url로 설정되어 있으므로 별도 변경 불필요

        response_api = session.post(api_url, data=api_params, headers=api_headers)
        response_api.raise_for_status()  # API 요청에 대한 HTTP 오류 확인

        # 3. JSON 응답에서 필요한 데이터 파싱
        data = response_api.json()

        result_code = data.get("resultCode")
        if result_code == "success" and data.get("result") is not None:
            nttCn_encoded = data["result"].get("nttCn", "")
            nttCn_decoded = html.unescape(nttCn_encoded)  # HTML 엔티티 디코딩

            print("\n--- HTML 원본 내용 ---")
            print(nttCn_decoded)

            # 불필요한 HTML 제거 및 텍스트 추출
            cleaned_text = clean_html_content(nttCn_decoded)

            print("\n--- 클린 텍스트 내용 ---")
            print(cleaned_text)
            return cleaned_text
        else:
            print(f"API 응답에서 성공적인 데이터를 찾을 수 없거나 결과가 null입니다: {data.get('msg', '메시지 없음')}")
            # JavaScript의 outptNoPlan()에 해당하는 "금일 일정이 없습니다." 메시지
            no_plan_html = "<div class='nowNaNoPlan' style='background-size: 35%; height: 470px;'>" \
                           "<span>금일 일정이 없습니다.</span>" \
                           "</div>"
            print(no_plan_html)
            return no_plan_html

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP 오류가 발생했습니다: {http_err} (상태 코드: {http_err.response.status_code})")
        print(f"응답 본문: {http_err.response.text}")  # 서버 응답 본문을 출력하여 추가 정보 확인
        return None
    except requests.exceptions.RequestException as e:
        print(f"웹페이지/API 접근 중 네트워크 오류가 발생했습니다: {e}")
        return None
    except Exception as e:
        print(f"데이터 처리 중 예상치 못한 오류가 발생했습니다: {e}")
        return None

def save_schedule_to_file(schedule_data):
    """
    오늘의 국회 일정을 텍스트 파일로 저장합니다.
    파일 이름은 'YYYY-MM-DD_오늘의국회.txt' 형식입니다.
    """
    date = datetime.now().strftime('%Y-%m-%d')
    file_path = f'../data/{date}_오늘의국회.txt'

    try:
        # 파일을 쓰기 모드('w')로 열고, UTF-8 인코딩을 사용하여 저장합니다.
        # 기존 파일이 있다면 덮어씁니다.
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(schedule_data)
        print(f"✅ 오늘의 국회 일정이 '{file_path}'에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"❌ 파일 저장 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    schedule_data = crawl_assembly_and_clean_text()
    save_schedule_to_file(schedule_data)



