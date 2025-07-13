import pandas as pd
import os
import time
import re
from xml.etree import ElementTree
import json
from tqdm.asyncio import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin, parse_qs, urlparse
import io
import asyncio
import aiohttp
import aiofiles
import aiofiles.os as aio_os

# --- PDF 추출을 위한 라이브러리 ---
# 실행 전 설치가 필요합니다: pip install pdfplumber
import pdfplumber

# --- 상수 정의 ---
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}
BASE_URL_SEARCH = "http://www.law.go.kr/DRF/lawSearch.do"
BASE_URL_SERVICE = "http://www.law.go.kr/DRF/lawService.do"
NTS_PDF_DOWNLOAD_URL = "https://taxlaw.nts.go.kr/downloadStorFile.do"
LAWGO_PDF_DOWNLOAD_URL = "https://www.law.go.kr/LSW/precPdfPrint.do"
BASE_OUTPUT_DIR = os.path.join("data", "raw")


class LawGovKrScraper:
    """
    대한민국 법제처 국가법령정보센터의 판례 데이터를 수집하고 파일로 저장하는 비동기 스크레이퍼 클래스.
    [개선] 보고서 집계 로직 수정.
    """

    def __init__(self, oc_id, request_delay=0.2, max_retries=3):
        if not oc_id:
            raise ValueError("OC ID (인증키)는 필수입니다.")
        self.oc_id = oc_id
        self.request_delay = request_delay
        self.max_retries = max_retries
        os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    async def _make_request(self, session, method, url, params=None, data=None, headers=None, allow_redirects=True):
        """중앙 집중식 비동기 요청 처리 메서드 (데이터 읽기까지 재시도 및 로그 정제)"""
        await asyncio.sleep(self.request_delay)
        last_exception = None
        req_headers = headers or session.headers
        
        for attempt in range(self.max_retries):
            try:
                async with session.request(method, url, params=params, data=data, headers=req_headers,
                                           allow_redirects=allow_redirects, timeout=30) as response:
                    response.raise_for_status()
                    content = await response.read()
                    return content, response.url
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    tqdm.write(f"❌ 최대 재시도({self.max_retries}회) 후 요청 최종 실패. URL: {url}, 오류: {last_exception}")
        return None, None

    async def _parse_list_response(self, response_content):
        """판례 목록 API의 XML 응답을 파싱합니다."""
        if not response_content: return [], 0
        try:
            root = ElementTree.fromstring(response_content)
            data = [{child.tag: child.text for child in item} for item in root.findall('prec')]
            total_count_element = root.find('totalCnt')
            total_count = int(total_count_element.text) if total_count_element is not None else 0
            return data, total_count
        except Exception as e:
            tqdm.write(f"  ❌ XML 파싱 중 오류 발생: {e}")
            return [], 0

    async def fetch_case_list(self, session, query=None, date=None, date_range=None, display=100, max_pages=None):
        """검색어 또는 판례 선고일자를 기반으로 판례 목록을 비동기로 수집합니다."""
        if not query and not date and not date_range:
            print("⚠️ 검색어(query), 선고일자(date), 또는 선고일자 범위(date_range) 중 하나는 반드시 입력해야 합니다.")
            return pd.DataFrame()

        search_desc = []
        if query: search_desc.append(f"검색어 '{query}'")
        if date: search_desc.append(f"선고일자 '{date}'")
        if date_range: search_desc.append(f"선고일자 범위 '{date_range}'")
        
        print(f"➡️ {', '.join(search_desc)}에 대한 판례 목록 수집 시작...")

        params = {'OC': self.oc_id, 'target': 'prec', 'type': 'xml', 'display': display, 'page': 1}
        if query: params['query'] = query
        if date: params['precJoYd'] = date.replace('.', '')
        if date_range: params['prncYd'] = date_range

        content, _ = await self._make_request(session, 'GET', BASE_URL_SEARCH, params=params)
        if not content: return pd.DataFrame()

        initial_data, total_count = await self._parse_list_response(content)
        if total_count == 0:
            print("⚠️ 검색 결과가 없습니다.")
            return pd.DataFrame()

        all_data = initial_data
        total_pages = (total_count + display - 1) // display
        pages_to_fetch = min(max_pages, total_pages) if max_pages is not None else total_pages
        print(f"📊 총 {total_count}개의 판례 발견. {pages_to_fetch}페이지에 걸쳐 수집합니다.")

        if pages_to_fetch > 1:
            tasks = [self._make_request(session, 'GET', BASE_URL_SEARCH, params={**params, 'page': i}) for i in range(2, pages_to_fetch + 1)]
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="📖 판례 목록 수집 중"):
                page_content, _ = await f
                if page_content:
                    data, _ = await self._parse_list_response(page_content)
                    if data: all_data.extend(data)
        
        df = pd.DataFrame(all_data)
        print(f"\n🎉 판례 목록 수집 완료! 총 {len(df)}건의 데이터를 수집했습니다.")
        return df

    async def _extract_text_from_pdf(self, pdf_content):
        """PDF 바이너리 데이터에서 텍스트를 추출합니다. (재시도 로직 추가)"""
        if not pdf_content: return None
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                with io.BytesIO(pdf_content) as pdf_file, pdfplumber.open(pdf_file) as pdf:
                    text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
                    if text: return text
                    last_exception = ValueError("PDF에서 텍스트를 추출하지 못했습니다 (내용이 비어있을 수 있음).")
            except Exception as e:
                last_exception = e
            if attempt < self.max_retries - 1: await asyncio.sleep(1)
        tqdm.write(f"❌ PDF 파싱 최종 실패 (시도 {self.max_retries}회). 오류: {last_exception}")
        return None
    
    def _sanitize_filename(self, filename):
        return re.sub(r'[\\/*?:"<>|]', "", str(filename)).strip() if filename else ""

    async def _download_pdf_from_nts(self, session, final_page_url, case_row):
        parsed_url = urlparse(str(final_page_url))
        query_params = parse_qs(parsed_url.query)
        ntst_dcm_id = query_params.get('ntstDcmId', [None])[0]
        if not ntst_dcm_id: return None
        
        post_data = {"data": json.dumps({"dcmDVO": {"ntstDcmId": ntst_dcm_id}}), "actionId": "ASIQTB002PR02", "fileType": "pdf", "fileName": case_row['사건명']}
        post_headers = {"Content-Type": "application/x-www-form-urlencoded", "Referer": str(final_page_url), "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}"}
        pdf_content, _ = await self._make_request(session, 'POST', NTS_PDF_DOWNLOAD_URL, data=post_data, headers=post_headers)
        return pdf_content

    async def _download_pdf_from_lawgo(self, session, final_page_url, case_row):
        post_data = {"precSeq": case_row['판례일련번호'], "fileType": "pdf", "preview": "N", "conJo": "1,2,3,4,5"}
        post_headers = {"Content-Type": "application/x-www-form-urlencoded", "Referer": str(final_page_url), "Origin": "https://www.law.go.kr"}
        pdf_content, _ = await self._make_request(session, 'POST', LAWGO_PDF_DOWNLOAD_URL, data=post_data, headers=post_headers)
        return pdf_content

    async def _fetch_and_save_case(self, session, case_row, period_output_dir):
        case_id = case_row['판례일련번호']
        params = {'OC': self.oc_id, 'target': 'prec', 'type': 'HTML', 'ID': case_id}
        
        shell_content, shell_url = await self._make_request(session, 'GET', BASE_URL_SERVICE, params=params)
        if not shell_content: return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row}

        soup = BeautifulSoup(shell_content, 'html.parser')
        url_input = soup.find('input', {'type': 'hidden', 'id': 'url'})
        lsw_url_path = url_input['value'] if url_input and 'value' in url_input.attrs else None
        if not lsw_url_path:
            iframe = soup.find('iframe')
            lsw_url_path = iframe['src'] if iframe and 'src' in iframe.attrs else None

        if not lsw_url_path: return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row}
        lsw_url = urljoin(str(shell_url), lsw_url_path)

        lsw_content, final_page_url = await self._make_request(session, 'GET', lsw_url, headers={'Referer': str(shell_url)})
        if not lsw_content: return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row}

        pdf_bytes = await self._download_pdf_from_nts(session, final_page_url, case_row) if "taxlaw.nts.go.kr" in str(final_page_url) else await self._download_pdf_from_lawgo(session, final_page_url, case_row)
        if not pdf_bytes: return {'status': 'DOWNLOAD_FAIL', 'case_info': case_row}
        
        text = await self._extract_text_from_pdf(pdf_bytes)
        if not text: return {'status': 'PARSE_FAIL', 'case_info': case_row}

        base_filename = f"{case_row['판례일련번호']}_{self._sanitize_filename(case_row['사건번호'])}"
        pdf_path = os.path.join(period_output_dir, f"{base_filename}.pdf")
        txt_path = os.path.join(period_output_dir, f"{base_filename}.txt")
        try:
            async with aiofiles.open(pdf_path, 'wb') as f: await f.write(pdf_bytes)
            async with aiofiles.open(txt_path, 'w', encoding='utf-8') as f: await f.write(text)
        except Exception as e:
            tqdm.write(f"  [실패] ID {case_id}: 파일 저장 중 오류 발생 - {e}")
            return {'status': 'SAVE_FAIL', 'case_info': case_row}
        
        return {'status': 'SUCCESS', 'text': text, 'case_info': case_row}

    async def _process_cases_batch(self, session, case_df, period_output_dir):
        """주어진 데이터프레임의 판례들을 수집하고 결과를 반환합니다."""
        if not isinstance(case_df, pd.DataFrame) or case_df.empty: return pd.DataFrame(), {}

        print(f"➡️ {len(case_df)}개 판례의 본문 수집 및 저장 시작...")
        
        tasks = [self._fetch_and_save_case(session, row, period_output_dir) for _, row in case_df.iterrows()]
        results = await tqdm.gather(*tasks, desc="✍️  판례 본문 수집/저장 중")

        status_map = {'SUCCESS': [], 'DOWNLOAD_FAIL': [], 'PARSE_FAIL': [], 'SAVE_FAIL': []}
        all_texts = []
        for res in results:
            # SKIPPED_EXISTS는 이 함수에서 처리하지 않으므로, 기본값으로 빈 리스트를 둡니다.
            status_map.setdefault(res['status'], []).append(res['case_info'])
            all_texts.append(res.get('text'))
        
        df_with_texts = case_df.copy()
        df_with_texts['판례본문'] = all_texts
        
        return df_with_texts, status_map

    async def _write_period_summary(self, period_output_dir, filename_suffix, status_map):
        """수집 기간에 대한 요약 리포트 파일을 작성합니다."""
        success_list = status_map.get('SUCCESS', [])
        skipped_list = status_map.get('SKIPPED_EXISTS', [])
        download_fail_list = status_map.get('DOWNLOAD_FAIL', [])
        parse_fail_list = status_map.get('PARSE_FAIL', [])
        save_fail_list = status_map.get('SAVE_FAIL', [])
        
        total_cases = sum(len(v) for v in status_map.values())
        newly_success_count = len(success_list)
        total_success_count = newly_success_count + len(skipped_list)
        success_rate = (total_success_count / total_cases * 100) if total_cases > 0 else 0
        
        summary_path = os.path.join(period_output_dir, f"crawling_result_{filename_suffix}.txt")
        async with aiofiles.open(summary_path, 'w', encoding='utf-8') as f:
            await f.write(f"--- 수집 결과 요약 ({filename_suffix}) ---\n")
            await f.write(f"총 대상: {total_cases}건\n")
            await f.write(f"최종 성공: {total_success_count}건 (신규 수집: {newly_success_count}건, 건너뛰기: {len(skipped_list)}건)\n")
            await f.write(f"수집 실패: {total_cases - total_success_count}건\n")
            await f.write(f"성공률: {success_rate:.2f}%\n")
            await f.write("\n--- 세부 내역 ---\n")
            
            if download_fail_list:
                await f.write(f"\n[다운로드 실패: {len(download_fail_list)}건]\n")
                for item in download_fail_list: await f.write(f"- ID: {item['판례일련번호']}, 사건번호: {item['사건번호']}\n")
            if parse_fail_list:
                await f.write(f"\n[PDF 파싱 실패: {len(parse_fail_list)}건]\n")
                for item in parse_fail_list: await f.write(f"- ID: {item['판례일련번호']}, 사건번호: {item['사건번호']}\n")
            if save_fail_list:
                await f.write(f"\n[파일 저장 실패: {len(save_fail_list)}건]\n")
                for item in save_fail_list: await f.write(f"- ID: {item['판례일련번호']}, 사건번호: {item['사건번호']}\n")
        print(f"📄 기간별 리포트가 '{summary_path}'에 저장되었습니다.")

def generate_filename_suffix(**kwargs):
    parts = [f"{k}_{v.replace('.', '-').replace('~', '-')}" for k, v in kwargs.items() if v]
    return "_".join(re.sub(r'[\\/*?:"<>|~]', "_", part) for part in parts)

async def main():
    MY_OC_ID = "leegy76"
    if MY_OC_ID == "YOUR_OC_ID_HERE":
        print("🚨 코드 실행 전에 'MY_OC_ID' 변수에 발급받은 인증키를 입력해주세요.")
        return

    async def _write_overall_summary(stats_list):
        """전체 수집 현황을 파일에 기록하는 헬퍼 함수"""
        print("\n" + "="*60 + "\n🔄️ 종합 수집 현황 업데이트...\n" + "="*60)
        successful_periods = [s for s in stats_list if s['status'] == 'SUCCESS']
        nodata_periods = [s for s in stats_list if s['status'] == 'NO_DATA']
        failed_periods = [s for s in stats_list if s['status'] == 'FATAL_ERROR']
        
        total_periods = len(stats_list)
        success_rate = (len(successful_periods) + len(nodata_periods)) / total_periods * 100 if total_periods > 0 else 0

        summary_text = [
            f"총 시도 기간: {total_periods}개",
            f"수집 성공 (데이터 있음): {len(successful_periods)}개",
            f"수집 성공 (데이터 없음): {len(nodata_periods)}개",
            f"수집 실패: {len(failed_periods)}개",
            f"성공률: {success_rate:.2f}%",
            "\n--- 기간별 상세 현황 ---"
        ]
        for stat in stats_list:
            period = stat['period']
            status = stat['status']
            if status == 'SUCCESS':
                s_map = stat['stats']
                total = sum(len(v) for v in s_map.values())
                success_count = len(s_map.get('SUCCESS', [])) + len(s_map.get('SKIPPED_EXISTS', []))
                summary_text.append(f"- {period}: 성공 (처리 {success_count}/{total}건)")
            else:
                summary_text.append(f"- {period}: {status}")

        report_str = "\n".join(summary_text)
        print(report_str)
        
        report_path = os.path.join(BASE_OUTPUT_DIR, "crawling_result_overall.txt")
        async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
            await f.write(report_str)
        print(f"\n💾 종합 리포트가 '{report_path}'에 업데이트되었습니다.")

    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
        scraper = LawGovKrScraper(oc_id=MY_OC_ID)

        async def run_collection(params, is_test=False):
            """단일 수집 작업을 실행하고 결과를 반환하는 함수"""
            filename_suffix = generate_filename_suffix(**params)
            period_output_dir = os.path.join(BASE_OUTPUT_DIR, filename_suffix)
            os.makedirs(period_output_dir, exist_ok=True)
            
            print(f"\n" + "="*50 + f"\n▶️ {'테스트' if is_test else '전체'} 수집 실행: {filename_suffix}\n" + "="*50)
            
            list_params = {'display': 5 if is_test else 100, 'max_pages': 1 if is_test else None}
            df_list = await scraper.fetch_case_list(session, **params, **list_params)

            if df_list is None or df_list.empty:
                print(f"\n⚠️ {filename_suffix} 조건에 해당하는 데이터가 없습니다.")
                return None

            print(f"\n[검증] 기존에 수집된 파일을 확인하여 누락된 항목만 선별합니다...")
            rows_to_collect = []
            rows_to_skip = []
            
            for _, row in df_list.iterrows():
                base_filename = f"{row['판례일련번호']}_{scraper._sanitize_filename(row['사건번호'])}"
                pdf_path = os.path.join(period_output_dir, f"{base_filename}.pdf")
                txt_path = os.path.join(period_output_dir, f"{base_filename}.txt")
                if await aio_os.path.exists(pdf_path) and await aio_os.path.exists(txt_path):
                    rows_to_skip.append(row)
                else:
                    rows_to_collect.append(row)
            
            status_map = {'SKIPPED_EXISTS': [dict(row) for row in rows_to_skip]}
            df_final_collected = pd.DataFrame()

            if rows_to_collect:
                df_to_collect = pd.DataFrame(rows_to_collect).reset_index(drop=True)
                df_final_collected, collected_status_map = await scraper._process_cases_batch(session, df_to_collect, period_output_dir)
                for key, value in collected_status_map.items():
                    status_map.setdefault(key, []).extend(value)
            else:
                print("  ✅ 모든 항목이 이미 수집되었습니다. 신규 수집을 건너뜁니다.")

            await scraper._write_period_summary(period_output_dir, filename_suffix, status_map)

            print(f"\n[통합] 기존 파일과 새로 수집된 데이터를 통합합니다...")
            skipped_texts = []
            for row_dict in status_map['SKIPPED_EXISTS']:
                base_filename = f"{row_dict['판례일련번호']}_{scraper._sanitize_filename(row_dict['사건번호'])}"
                txt_path = os.path.join(period_output_dir, f"{base_filename}.txt")
                try:
                    async with aiofiles.open(txt_path, 'r', encoding='utf-8') as f:
                        text = await f.read()
                    skipped_texts.append(text)
                except Exception:
                    skipped_texts.append(None)
            
            df_skipped = pd.DataFrame(status_map['SKIPPED_EXISTS'])
            if not df_skipped.empty:
                df_skipped['판례본문'] = skipped_texts
            
            df_final_total = pd.concat([df_final_collected, df_skipped], ignore_index=True)
            if not df_final_total.empty:
                df_final_total = df_final_total.set_index('판례일련번호').loc[df_list['판례일련번호'].astype(str)].reset_index()
                json_path = os.path.join(period_output_dir, f"collected_cases_{filename_suffix}.json")
                df_final_total.to_json(json_path, orient='records', force_ascii=False, indent=4)
                print(f"\n💾 최종 통합 결과(JSON)가 '{json_path}'에 저장되었습니다.")
            
            return status_map

        # # --- 테스트 케이스 실행 (필요시 주석 해제) ---
        # await run_collection({"query": "손해배상"}, is_test=True)

        # --- 실제 데이터 수집 로직 ---
        print("\n" + "="*60 + "\n▶️ 실제 데이터 수집 실행 (2000.01.01 ~ 2025.07.10)\n" + "="*60)
        start_date = pd.to_datetime("2000-01-01")
        end_date = pd.to_datetime("2025-07-10")
        
        date_ranges = pd.date_range(start=start_date, end=end_date, freq='MS')
        
        overall_stats = []
        collection_max_retries = 3

        for i in range(len(date_ranges)):
            range_start = date_ranges[i]
            range_end = (range_start + pd.offsets.MonthEnd(1)) if i < len(date_ranges) - 1 else end_date
            if range_end > end_date: range_end = end_date
            date_range_str = f"{range_start.strftime('%Y%m%d')}~{range_end.strftime('%Y%m%d')}"
            
            for attempt in range(collection_max_retries):
                try:
                    status_map = await run_collection({"date_range": date_range_str})
                    if status_map is not None:
                        overall_stats.append({'period': date_range_str, 'status': 'SUCCESS', 'stats': status_map})
                    else:
                         overall_stats.append({'period': date_range_str, 'status': 'NO_DATA', 'stats': {}})
                    break 
                except Exception as e:
                    print(f"‼️ {date_range_str} 기간 수집 중 심각한 오류 발생 (시도 {attempt + 1}/{collection_max_retries}): {e}")
                    if attempt < collection_max_retries - 1:
                        await asyncio.sleep(1)
                    else:
                        print(f"‼️ {date_range_str} 기간 수집에 최종 실패했습니다.")
                        overall_stats.append({'period': date_range_str, 'status': 'FATAL_ERROR', 'stats': {}})
            
            await _write_overall_summary(overall_stats)


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
