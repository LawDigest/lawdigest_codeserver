"""
LawGovKrScraper

이 모듈은 대한민국 법제처 국가법령정보센터의 판례 데이터를 비동기 방식으로 수집하고 PDF를 텍스트로 변환하여 저장하는 기능을 제공합니다.

주요 기능:
- 판례 목록 조회 및 페이징
- PDF 다운로드 및 텍스트 추출
- 파일 기반 중복 수집 방지
- 속도 제한 및 허용 시간 제약
- 로깅 및 리포트 생성
- 테스트 모드 지원
"""
import os
import re
import time
import datetime
import io
import json
import logging
import asyncio
from collections import deque
from xml.etree import ElementTree
from urllib.parse import urljoin, parse_qs, urlparse

import pandas as pd
import aiohttp
import aiofiles
import aiofiles.os as aio_os
from bs4 import BeautifulSoup
import pdfplumber
from tqdm import tqdm

# --- 설정 (Configuration) ---
# 기본 출력 디렉토리. 수집된 데이터는 'data/raw'에 저장됩니다.
BASE_OUTPUT_DIR = os.path.join("data", "raw")
# 재수집 시도 시 데이터가 저장될 디렉토리 (현재는 사용되지 않음)
RECOLLECT_DIR = os.path.join(BASE_OUTPUT_DIR, "recollected_data")

# 기본 HTTP 요청 헤더
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ko-KR,ko;q=0.9',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0' # 봇 차단을 피하기 위해 일반 브라우저처럼 보이게 설정
}
# 법제처 API URL
BASE_URL_SEARCH = "http://www.law.go.kr/DRF/lawSearch.do" # 목록 검색용
BASE_URL_SERVICE = "http://www.law.go.kr/DRF/lawService.do" # 상세 정보용
# PDF 다운로드 URL
NTS_PDF_DOWNLOAD_URL = "https://taxlaw.nts.go.kr/downloadStorFile.do" # 국세청 판례
LAWGO_PDF_DOWNLOAD_URL = "https://www.law.go.kr/LSW/precPdfPrint.do" # 법제처 판례

# --- 로깅 설정 (Logging Setup) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

class RateLimiter:
    """
    비동기 환경에서 API 요청 속도를 제어하는 클래스입니다.
    주어진 시간(per_seconds) 동안 최대 호출 횟수(max_calls)를 제한하여 서버 부하를 줄입니다.

    사용법:
        limiter = RateLimiter(max_calls=3, per_seconds=2)
        async with limiter:
            # 이 블록 안의 코드는 2초에 3번 이하로만 실행됩니다.
            await session.get(url)
    """
    def __init__(self, max_calls: int, per_seconds: float):
        self.max_calls = max_calls
        self.per_seconds = per_seconds
        self.calls = deque()

    async def __aenter__(self):
        """async with 진입 시 호출 속도를 체크하고 필요시 대기합니다."""
        now = time.monotonic()
        # 오래된 호출 기록 제거
        while self.calls and now - self.calls[0] > self.per_seconds:
            self.calls.popleft()
        
        # 제한 횟수 초과 시 대기
        if len(self.calls) >= self.max_calls:
            sleep_time = self.per_seconds - (now - self.calls[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.calls.append(time.monotonic())

    async def __aexit__(self, exc_type, exc, tb):
        """async with 블록을 빠져나갈 때 별도 작업은 없습니다."""
        pass

async def wait_for_window(start_hour=3, end_hour=9):
    """
    지정된 시간 창(기본: 새벽 3시 ~ 오전 9시)에만 작업이 실행되도록 대기합니다.
    서버 부하가 적은 시간대에 크롤링을 실행하기 위함입니다.
    
    테스트 모드에서는 이 함수가 호출되지 않습니다.
    """
    now = datetime.datetime.now()
    if not (start_hour <= now.hour < end_hour):
        target_time = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        # 현재 시간이 허용 종료 시간보다 늦으면, 다음 날 시작 시간으로 설정
        if now.hour >= end_hour:
            target_time += datetime.timedelta(days=1)
        
        wait_seconds = (target_time - now).total_seconds()
        logging.info(f"작업 허용 시간이 아닙니다. {int(wait_seconds // 3600)}시간 {int((wait_seconds % 3600) // 60)}분 후 작업을 시작합니다.")
        await asyncio.sleep(wait_seconds)

class LawGovKrScraper:
    """
    국가법령정보센터 판례 수집기 클래스.

    이 클래스는 판례 목록 조회, 개별 판례의 PDF 다운로드, 텍스트 추출,
    파일 저장 등 스크래핑의 모든 과정을 관리합니다.

    Args:
        oc_id (str): 법제처 Open API 인증키.
        request_delay (float): 각 HTTP 요청 사이의 최소 지연 시간 (초).
        max_retries (int): 네트워크 오류 발생 시 최대 재시도 횟수.
    """
    def __init__(self, oc_id: str, request_delay: float = 0.2, max_retries: int = 3):
        if not oc_id:
            raise ValueError("OC ID (인증키)는 필수입니다. API 키를 인자로 전달해주세요.")
        self.oc_id = oc_id
        self.request_delay = request_delay
        self.max_retries = max_retries
        # 데이터 저장을 위한 디렉토리 생성
        os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
        os.makedirs(RECOLLECT_DIR, exist_ok=True)

    async def _make_request(self, session: aiohttp.ClientSession, method: str,
                            url: str, params=None, data=None,
                            headers=None, allow_redirects=True):
        """
        비동기 HTTP 요청을 보내는 내부 헬퍼 메서드.
        RateLimiter, 재시도 로직, 에러 로깅이 포함되어 있습니다.

        Returns:
            tuple[bytes | None, aiohttp.ClientResponse.url | None]: (응답 본문, 최종 URL)
        """
        await asyncio.sleep(self.request_delay)
        last_exception = None
        request_headers = headers or session.headers

        for attempt in range(self.max_retries):
            try:
                # 2초에 3번으로 요청 속도 제한
                async with RateLimiter(max_calls=3, per_seconds=2):
                    async with session.request(
                        method, url, params=params, data=data,
                        headers=request_headers, allow_redirects=allow_redirects,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        response.raise_for_status() # 200번대 응답이 아니면 예외 발생
                        return await response.read(), response.url
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                logging.warning(f"요청 실패 (시도 {attempt + 1}/{self.max_retries}): {url}, 오류: {e}")
                await asyncio.sleep(2 ** attempt) # 재시도 간격 증가 (Exponential backoff)
        
        logging.error(f"최대 재시도({self.max_retries}회) 후에도 요청에 실패했습니다. URL: {url}, 최종 오류: {last_exception}")
        return None, None

    async def _parse_list_response(self, content: bytes):
        """
        판례 목록 API의 XML 응답을 파싱합니다.

        Args:
            content (bytes): XML 형식의 응답 본문.

        Returns:
            tuple[list[dict], int]: (판례 데이터 리스트, 전체 판례 수)
        """
        if not content:
            return [], 0
        try:
            root = ElementTree.fromstring(content)
            # 'prec' 태그가 각 판례 정보를 담고 있음
            data = [{child.tag: child.text for child in item} for item in root.findall('prec')]
            total_count_element = root.find('totalCnt')
            total_count = int(total_count_element.text) if total_count_element is not None else 0
            return data, total_count
        except ElementTree.ParseError as e:
            logging.error(f"XML 파싱 오류가 발생했습니다: {e}")
            # API 에러 메시지 확인
            try:
                root = ElementTree.fromstring(content)
                error_msg = root.find('.//resultMessage') or root.find('.//MESSAGE')
                if error_msg is not None:
                    logging.error(f"API 에러 메시지: {error_msg.text}")
            except Exception:
                pass
            return [], 0

    async def fetch_case_list(self, session: aiohttp.ClientSession,
                              query=None, date=None, date_range=None,
                              display=100, max_pages=None):
        """
        판례 목록을 조회하고 모든 페이지를 순회하며 데이터를 수집합니다.

        Args:
            session: aiohttp 클라이언트 세션.
            query (str, optional): 검색어.
            date (str, optional): 선고일자 (예: '2023.01.01').
            date_range (str, optional): 판시사항 게재일자 범위 (예: '20230101~20230131').
            display (int): 한 페이지에 표시할 항목 수 (최대 100).
            max_pages (int, optional): 수집할 최대 페이지 수. None이면 전체 페이지를 수집.

        Returns:
            pd.DataFrame: 수집된 판례 목록 데이터프레임.
        """
        if not any([query, date, date_range]):
            logging.warning("검색 조건(query, date, date_range) 중 하나는 반드시 필요합니다.")
            return pd.DataFrame()

        params = {
            'OC': self.oc_id,
            'target': 'prec',
            'type': 'xml',
            'display': display,
            'page': 1
        }
        if query: params['query'] = query
        if date: params['precJoYd'] = date.replace('.', '')
        if date_range: params['prncYd'] = date_range

        logging.info(f"판례 목록 수집을 시작합니다. 검색 조건: { {k:v for k,v in params.items() if k not in ['OC', 'target']} }")
        
        # 첫 페이지 요청으로 전체 개수 확인
        content, _ = await self._make_request(session, 'GET', BASE_URL_SEARCH, params=params)
        
        if content:
            try:
                root = ElementTree.fromstring(content)
                msg = root.find('message') or root.find('msg')
                if msg is not None and msg.text:
                    logging.info(f"API 응답 메시지: {msg.text}")
            except Exception:
                pass

        initial_data, total_items = await self._parse_list_response(content)
        if total_items == 0:
            logging.info("검색된 판례가 없습니다.")
            return pd.DataFrame()

        total_pages = (total_items + display - 1) // display
        pages_to_fetch = min(max_pages, total_pages) if max_pages else total_pages
        logging.info(f"총 {total_items}건의 판례를 찾았습니다. {pages_to_fetch}페이지를 수집합니다.")

        all_data = initial_data
        
        # 2페이지부터 순차적으로 요청 (API가 동시 요청을 허용하지 않을 수 있음)
        if pages_to_fetch > 1:
            pbar = tqdm(range(2, pages_to_fetch + 1), desc="➡️  판례 목록 페이징")
            for page in pbar:
                params['page'] = page
                pbar.set_postfix_str(f"페이지 {page}/{pages_to_fetch}")
                page_content, _ = await self._make_request(session, 'GET', BASE_URL_SEARCH, params=params)
                data, _ = await self._parse_list_response(page_content)
                all_data.extend(data)
                await asyncio.sleep(0.5) # 페이지 간 예의 있는 딜레이

        df = pd.DataFrame(all_data)
        logging.info(f"판례 목록 수집 완료: 총 {len(df)}건")
        return df

    def _sanitize_filename(self, name: str) -> str:
        """파일 이름으로 사용할 수 없는 특수문자를 '_'로 변경합니다."""
        return re.sub(r'[\\/*?:"<>|]', '_', str(name)).strip()

    async def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str | None:
        """
        PDF 파일의 바이너리 데이터에서 텍스트를 추출합니다.
        실패 시 재시도 로직이 포함되어 있습니다.
        """
        if not pdf_bytes:
            return None
        
        for attempt in range(self.max_retries):
            try:
                # pdfplumber를 사용하여 텍스트 추출
                with io.BytesIO(pdf_bytes) as pdf_file, pdfplumber.open(pdf_file) as pdf:
                    full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                    return full_text
            except Exception as e:
                logging.warning(f"PDF 텍스트 추출 실패 (시도 {attempt + 1}/{self.max_retries}): {e}")
                await asyncio.sleep(1)
        
        logging.error("최대 재시도 후에도 PDF 텍스트 추출에 최종 실패했습니다.")
        return None

    async def _download_pdf_from_nts(self, session, final_url, case_row):
        """국세청(NTS) 웹사이트로부터 PDF를 다운로드합니다."""
        parsed_url = urlparse(str(final_url))
        query_params = parse_qs(parsed_url.query)
        ntst_dcm_id = query_params.get('ntstDcmId', [None])[0]
        
        if not ntst_dcm_id:
            logging.warning(f"국세청 PDF 다운로드에 필요한 'ntstDcmId'를 찾을 수 없습니다. URL: {final_url}")
            return None
            
        post_data = {
            'data': json.dumps({'dcmDVO': {'ntstDcmId': ntst_dcm_id}}),
            'actionId': 'ASIQTB002PR02',
            'fileType': 'pdf',
            'fileName': case_row.get('사건명', 'download')
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': str(final_url),
            'Origin': f"{parsed_url.scheme}://{parsed_url.netloc}"
        }
        
        pdf_bytes, _ = await self._make_request(
            session, 'POST', NTS_PDF_DOWNLOAD_URL, data=post_data, headers=headers
        )
        return pdf_bytes

    async def _download_pdf_from_lawgo(self, session, final_url, case_row):
        """법제처(law.go.kr) 웹사이트로부터 PDF를 다운로드합니다."""
        post_data = {
            'precSeq': case_row['판례일련번호'],
            'fileType': 'pdf',
            'preview': 'N',
            'conJo': '1,2,3,4,5' # PDF에 포함할 항목 (판시사항, 판결요지 등)
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': str(final_url),
            'Origin': 'https://www.law.go.kr'
        }
        
        pdf_bytes, _ = await self._make_request(
            session, 'POST', LAWGO_PDF_DOWNLOAD_URL, data=post_data, headers=headers
        )
        return pdf_bytes

    async def _fetch_and_save_case(self, session, case_row, output_dir):
        """
        개별 판례의 상세 정보를 조회하여 PDF를 다운로드하고 텍스트를 추출하여 저장합니다.
        
        진행 과정:
        1. 판례일련번호로 상세 정보 페이지(껍데기) 요청
        2. 페이지 HTML에서 실제 내용이 담긴 iframe URL 추출
        3. iframe URL로 접근하여 최종 콘텐츠 페이지 URL 획득 (리디렉션 처리)
        4. 최종 URL이 국세청/법제처인지에 따라 다른 방식으로 PDF 다운로드
        5. 다운로드한 PDF에서 텍스트 추출
        6. PDF와 TXT 파일을 지정된 디렉토리에 저장

        Returns:
            dict: 작업 결과 (상태, 판례 정보, 추출된 텍스트 등)
        """
        case_id = case_row['판례일련번호']
        
        # 1. 상세 정보 페이지(껍데기) 요청
        params = {'OC': self.oc_id, 'target': 'prec', 'type': 'HTML', 'ID': case_id}
        shell_content, shell_url = await self._make_request(
            session, 'GET', BASE_URL_SERVICE, params=params
        )
        if not shell_content:
            return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row, 'reason': 'Shell page fetch failed'}

        # 2. iframe URL 추출
        soup = BeautifulSoup(shell_content, 'html.parser')
        url_input = soup.find('input', {'type': 'hidden', 'id': 'url'})
        path = url_input['value'] if url_input else None
        if not path:
            iframe = soup.find('iframe')
            path = iframe['src'] if iframe else None
        
        if not path:
            return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row, 'reason': 'Could not find content URL/iframe'}

        # 3. 실제 콘텐츠 페이지 URL 획득
        page_url = urljoin(str(shell_url), path)
        _, final_url = await self._make_request(
            session, 'GET', page_url, headers={'Referer': str(shell_url)}
        )
        if not final_url:
            return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row, 'reason': 'Content page fetch failed'}

        # 4. PDF 다운로드
        if 'taxlaw.nts.go.kr' in str(final_url):
            pdf_bytes = await self._download_pdf_from_nts(session, final_url, case_row)
        else:
            pdf_bytes = await self._download_pdf_from_lawgo(session, final_url, case_row)
        
        if not pdf_bytes:
            return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row, 'reason': 'PDF download failed'}

        # 5. 텍스트 추출
        text = await self._extract_text_from_pdf(pdf_bytes)
        if not text:
            return {'status': 'PARSE_FAIL', 'case_info': case_row}

        # 6. 파일 저장
        filename = f"{case_id}_{self._sanitize_filename(case_row.get('사건번호', ''))}"
        pdf_path = os.path.join(output_dir, f"{filename}.pdf")
        txt_path = os.path.join(output_dir, f"{filename}.txt")
        
        try:
            async with aiofiles.open(pdf_path, 'wb') as f:
                await f.write(pdf_bytes)
            async with aiofiles.open(txt_path, 'w', encoding='utf-8') as f:
                await f.write(text)
        except Exception as e:
            logging.error(f"파일 저장 중 오류 발생 (ID: {case_id}): {e}")
            # 실패 시 생성된 파일 삭제 시도
            if await aio_os.path.exists(pdf_path): await aio_os.remove(pdf_path)
            if await aio_os.path.exists(txt_path): await aio_os.remove(txt_path)
            return {'status': 'SAVE_FAIL', 'case_info': case_row}

        return {'status': 'SUCCESS', 'case_info': case_row, 'text': text}

    async def _process_cases_batch(self, session, df, output_dir):
        """
        판례 목록(DataFrame)을 비동기적으로 처리하고 진행 상황을 표시합니다.
        """
        if df.empty:
            return pd.DataFrame(), {}
            
        logging.info(f"판례 본문 수집을 시작합니다: {len(df)}건 -> {output_dir}")
        
        tasks = [self._fetch_and_save_case(session, row, output_dir) for _, row in df.iterrows()]
        
        results = []
        with tqdm(total=len(tasks), desc="✍️  판례 본문 수집") as pbar:
            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)
                pbar.update(1)

        status_map = {
            'SUCCESS': [], 'DOWNLOAD_FAIL': [],
            'PARSE_FAIL': [], 'SAVE_FAIL': [], 'SKIPPED_EXISTS': []
        }
        texts = []
        # 결과를 case_id 기준으로 매핑하기 위한 딕셔너리
        result_map = {r['case_info']['판례일련번호']: r for r in results}

        for _, row in df.iterrows():
            case_id = row['판례일련번호']
            result = result_map.get(case_id)
            if result:
                status_map.setdefault(result['status'], []).append(result['case_info'])
                texts.append(result.get('text'))
            else:
                # 이 경우는 거의 발생하지 않아야 함
                texts.append(None)

        out_df = df.copy()
        out_df['판례본문'] = texts
        return out_df, status_map

    async def _write_period_summary(self, output_dir, suffix, status_map):
        """
        기간별 수집 결과를 요약한 리포트 파일을 생성합니다.
        """
        success_list = status_map.get('SUCCESS', [])
        skipped_list = status_map.get('SKIPPED_EXISTS', [])
        total_success = len(success_list) + len(skipped_list)
        total_processed = sum(len(v) for v in status_map.values())
        fail_count = total_processed - total_success

        report_path = os.path.join(output_dir, f"crawling_result_{suffix}.txt")
        async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
            await f.write(f"--- 수집 요약 ({suffix}) ---\n")
            await f.write(f"총 대상: {total_processed}건\n")
            await f.write(f"최종 성공: {total_success}건 (신규: {len(success_list)}건, 중복/건너뜀: {len(skipped_list)}건)\n")
            await f.write(f"수집 실패: {fail_count}건\n\n")

            if success_list:
                await f.write(f"[신규 수집 성공: {len(success_list)}건]\n")
                for item in success_list:
                    await f.write(f"- ID: {item['판례일련번호']}, 사건번호: {item['사건번호']}\n")
                await f.write("\n")
            
            # 실패 상세 내역 작성
            for key, label in [('DOWNLOAD_FAIL', '다운로드 실패'),
                               ('PARSE_FAIL', '텍스트 추출 실패'),
                               ('SAVE_FAIL', '파일 저장 실패')]:
                arr = status_map.get(key, [])
                if arr:
                    await f.write(f"[{label}: {len(arr)}건]\n")
                    for item in arr:
                        await f.write(f"- ID: {item['판례일련번호']}, 사건번호: {item['사건번호']}\n")
                    await f.write("\n")
        
        logging.info(f"기간별 리포트 작성 완료: {report_path}")

async def run_scraper(oc_id: str, query: str = None, date: str = None, 
                      date_range: str = None, test_mode: bool = False):
    """
    판례 스크래퍼를 실행하는 메인 함수.

    Args:
        oc_id (str): 법제처 Open API 인증키 (필수).
        query (str, optional): 검색어. Defaults to None.
        date (str, optional): 선고일자 (예: '2023.12.25'). Defaults to None.
        date_range (str, optional): 판시사항 게재일자 범위 (예: '20230101~20231231'). Defaults to None.
        test_mode (bool, optional): 테스트 모드 활성화 여부. Defaults to False.
            - True일 경우, 시간 제약 없이 '판례' 검색어로 1페이지만 수집.
    """
    # --- 모드 설정: 일반 모드 vs 테스트 모드 ---
    if test_mode:
        logging.info("--- 🧪 테스트 모드 활성화 🧪 ---")
        # 테스트 모드용 파라미터 설정
        query_param = "판례"
        date_param = None
        date_range_param = None
        max_pages = 1  # 테스트 시에는 1페이지만 수집
        output_suffix = f"test_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    else:
        # 일반 모드용 파라미터 설정
        if not any([query, date, date_range]):
            logging.error("일반 모드에서는 query, date, date_range 중 하나 이상의 검색 조건이 필요합니다.")
            return
        query_param = query
        date_param = date
        date_range_param = date_range
        max_pages = None # 전체 페이지 수집
        output_suffix = date.replace('.', '') if date else (date_range or "query_search")

    # --- 크롤러 실행 ---
    scraper = LawGovKrScraper(oc_id=oc_id)

    # 테스트 모드가 아닐 때만 작업 허용 시간까지 대기
    if not test_mode:
        await wait_for_window()

    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
        # 1. 판례 목록 가져오기
        df_list = await scraper.fetch_case_list(
            session, query=query_param, date=date_param, 
            date_range=date_range_param, max_pages=max_pages
        )

        if df_list.empty:
            logging.info("수집할 판례 목록이 없어 프로그램을 종료합니다.")
            return

        # 2. 파일 기반 중복 체크 후, 누락된 판례만 수집
        output_dir = os.path.join(BASE_OUTPUT_DIR, output_suffix)
        os.makedirs(output_dir, exist_ok=True)
        
        cases_to_process = []
        skipped_case_infos = []
        logging.info(f"기존 파일 확인 및 수집 대상 필터링 중... (총 {len(df_list)}건)")
        for _, row in df_list.iterrows():
            filename = f"{row['판례일련번호']}_{scraper._sanitize_filename(row.get('사건번호', ''))}"
            txt_path = os.path.join(output_dir, f"{filename}.txt")
            
            if os.path.exists(txt_path):
                skipped_case_infos.append(row.to_dict())
            else:
                cases_to_process.append(row.to_dict())

        logging.info(f"중복 파일 제외, 신규 수집 대상: {len(cases_to_process)}건 (건너뛰기: {len(skipped_case_infos)}건)")

        status_map = {}
        if cases_to_process:
            df_to_process = pd.DataFrame(cases_to_process)
            _, status_map = await scraper._process_cases_batch(session, df_to_process, output_dir)
        else:
            logging.info("신규로 수집할 판례가 없습니다.")
        
        # 건너뛴 항목을 status_map에 추가하여 리포트에 반영
        status_map['SKIPPED_EXISTS'] = skipped_case_infos
        
        # 3. 수집 결과 리포트 작성
        await scraper._write_period_summary(output_dir, output_suffix, status_map)
        
        # 4. 최종 결과 로깅 (재수집 로직은 제거됨)
        all_failed_items = (status_map.get('DOWNLOAD_FAIL', []) + 
                            status_map.get('PARSE_FAIL', []) + 
                            status_map.get('SAVE_FAIL', []))

        if not all_failed_items and cases_to_process:
            logging.info("모든 신규 항목이 성공적으로 수집되었습니다.")
        elif not all_failed_items and not cases_to_process:
            logging.info("처리할 신규 항목이 없었고, 실패도 없었습니다.")
        else:
            logging.warning(f"총 {len(all_failed_items)}건의 항목 수집에 실패했습니다. 상세 내용은 리포트를 확인하세요.")

    logging.info("✨ 모든 작업이 완료되었습니다. ✨")


if __name__ == "__main__":
    # --- 실행 예시 ---
    # 스크립트를 실행하기 전, 아래 YOUR_OC_ID 변수에 실제 법제처 Open API 인증키를 입력해야 합니다.
    
    YOUR_OC_ID = "leegy76" 

    if YOUR_OC_ID == "YOUR_API_KEY_HERE":
        logging.error("스크립트를 실행하기 전에 'YOUR_OC_ID' 변수에 실제 API 인증키를 입력해야 합니다.")
    else:
        # --- 실행 옵션 (아래 중 실행하려는 옵션의 주석을 해제하세요) ---
        try:
            # 옵션 1: 테스트 모드로 실행 (시간 제약 없이 '판례' 검색어로 1페이지만 수집)
            asyncio.run(run_scraper(oc_id=YOUR_OC_ID, test_mode=True))

            # 옵션 2: 특정 날짜로 검색하여 실행
            # asyncio.run(run_scraper(oc_id=YOUR_OC_ID, date="2023.01.05"))

            # 옵션 3: 특정 기간으로 검색하여 실행
            # asyncio.run(run_scraper(oc_id=YOUR_OC_ID, date_range="20230101~20230131"))
            
            # 옵션 4: 특정 검색어로 실행
            # asyncio.run(run_scraper(oc_id=YOUR_OC_ID, query="민법"))

        except KeyboardInterrupt:
            logging.info("사용자에 의해 프로그램이 중단되었습니다.")
        except Exception as e:
            logging.error(f"실행 중 오류가 발생했습니다: {e}")
