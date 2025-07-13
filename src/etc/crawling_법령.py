import pandas as pd
import os
import time
import re
from xml.etree import ElementTree # 법령 목록 조회를 위해 다시 사용
import json
from tqdm.asyncio import tqdm
from urllib.parse import urljoin, parse_qs, urlparse
import io
import asyncio
import aiohttp
import aiofiles
import aiofiles.os as aio_os

# --- 사용자 정의 예외 ---
class IPBlockedError(Exception):
    """IP 접근 제한 시 발생하는 사용자 정의 예외"""
    pass

# --- 상수 정의 ---
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}
BASE_URL_SEARCH = "http://www.law.go.kr/DRF/lawSearch.do"
BASE_URL_SERVICE = "http://www.law.go.kr/DRF/lawService.do"
BASE_OUTPUT_DIR = os.path.join("data", "raw_laws")


class LawScraper:
    """
    대한민국 법제처 국가법령정보센터의 법령 데이터를 수집하는 비동기 스크레이퍼.
    (IP 차단 방지를 위한 동시성 제어 및 재시도 로직 강화)
    """

    def __init__(self, oc_id, request_delay=1.0, max_retries=3, max_concurrency=5):
        if not oc_id:
            raise ValueError("OC ID (인증키)는 필수입니다.")
        self.oc_id = oc_id
        self.request_delay = request_delay # 요청 간 딜레이 증가
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(max_concurrency) # 동시 요청 수 제한

        os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    async def _make_request(self, session, method, url, params=None, data=None, headers=None, allow_redirects=True):
        """중앙 집중식 비동기 요청 처리 메서드 (동시성 제어 및 IP 차단 감지 추가)"""
        async with self.semaphore: # 한 번에 정해진 수의 요청만 실행
            await asyncio.sleep(self.request_delay)
            last_exception = None
            req_headers = headers or session.headers
            
            for attempt in range(self.max_retries):
                try:
                    async with session.request(method, url, params=params, data=data, headers=req_headers,
                                               allow_redirects=allow_redirects, timeout=30) as response:
                        response.raise_for_status()
                        content = await response.read()

                        # IP 차단 메시지 확인
                        if b'IP' in content and b'alert' in content:
                            decoded_content = content.decode('utf-8', errors='ignore')
                            if '접근제한된 IP 입니다' in decoded_content:
                                raise IPBlockedError("IP가 차단되었습니다.")

                        return content, response.url
                except (aiohttp.ClientError, asyncio.TimeoutError, IPBlockedError) as e:
                    last_exception = e
                    tqdm.write(f"⚠️ 요청 실패 (시도 {attempt + 1}/{self.max_retries}): {e}")
                    if attempt < self.max_retries - 1:
                        # 지수 백오프: 2, 4, 8초... 순으로 대기 시간 증가
                        wait_time = 2 ** (attempt + 1) 
                        tqdm.write(f"   ➡️ {wait_time}초 후 재시도합니다...")
                        await asyncio.sleep(wait_time)
                    else:
                        tqdm.write(f"❌ 최대 재시도({self.max_retries}회) 후 요청 최종 실패. URL: {url}, 오류: {last_exception}")
            return None, None

    async def _parse_law_list_xml_response(self, response_content):
        """법령 목록 API의 XML 응답을 파싱합니다."""
        if not response_content: return [], 0
        try:
            root = ElementTree.fromstring(response_content)
            data = [{child.tag: child.text for child in item} for item in root.findall('law')]
            total_count_element = root.find('totalCnt')
            total_count = int(total_count_element.text) if total_count_element is not None else 0
            return data, total_count
        except Exception as e:
            tqdm.write(f" ❌ XML 파싱 중 오류 발생: {e}")
            return [], 0

    async def fetch_law_list(self, session, efyd_range=None, display=100, max_pages=None):
        """시행일자 범위를 기반으로 법령 목록을 비동기로 수집합니다. (XML 방식)"""
        if not efyd_range:
            print("⚠️ 시행일자 범위(efyd_range)는 반드시 입력해야 합니다.")
            return pd.DataFrame()

        print(f"➡️ 시행일자 범위 '{efyd_range}'에 대한 법령 목록 수집 시작... (XML)")

        params = {
            'OC': self.oc_id, 
            'target': 'law', 
            'type': 'xml',
            'display': display, 
            'page': 1,
            'sort': 'efdesc',
            'efYd': efyd_range 
        }

        content, _ = await self._make_request(session, 'GET', BASE_URL_SEARCH, params=params)
        if not content: return pd.DataFrame()

        initial_data, total_count = await self._parse_law_list_xml_response(content)
        if total_count == 0:
            print("⚠️ 검색 결과가 없습니다.")
            return pd.DataFrame()

        all_data = initial_data
        total_pages = (total_count + display - 1) // display
        pages_to_fetch = min(max_pages, total_pages) if max_pages is not None else total_pages
        print(f"📊 총 {total_count}개의 법령 발견. {pages_to_fetch}페이지에 걸쳐 수집합니다.")

        if pages_to_fetch > 1:
            tasks = [self._make_request(session, 'GET', BASE_URL_SEARCH, params={**params, 'page': i}) for i in range(2, pages_to_fetch + 1)]
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="📖 법령 목록 수집 중"):
                page_content, _ = await f
                if page_content:
                    data, _ = await self._parse_law_list_xml_response(page_content)
                    if data: all_data.extend(data)
        
        df = pd.DataFrame(all_data)
        print(f"\n🎉 법령 목록 수집 완료! 총 {len(df)}건의 데이터를 수집했습니다.")
        return df

    def _sanitize_filename(self, filename):
        return re.sub(r'[\\/*?:"<>|]', "", str(filename)).strip() if filename else ""

    @staticmethod
    def _parse_law_article_parts(data, indent="  "):
        """항/호 등 조문의 하위 구조를 재귀적으로 파싱하여 텍스트로 변환합니다."""
        parts = []
        if '항' in data:
            항_list = data['항'] if isinstance(data['항'], list) else [data['항']]
            for 항 in 항_list:
                if '항내용' in 항:
                    항_번호 = 항.get('항번호', '')
                    항_내용 = f"{indent}{항_번호} {항['항내용'].strip()}" if 항_번호 else f"{indent}{항['항내용'].strip()}"
                    parts.append(항_내용)
                parts.extend(LawScraper._parse_law_article_parts(항, indent + "  "))
        if '호' in data:
            호_list = data['호'] if isinstance(data['호'], list) else [data['호']]
            for 호 in 호_list:
                if '호내용' in 호:
                    호_번호 = 호.get('호번호', '')
                    호_내용 = f"{indent}{호_번호} {호['호내용'].strip()}" if 호_번호 else f"{indent}{호['호내용'].strip()}"
                    parts.append(호_내용)
        return parts

    async def _format_json_to_text(self, json_data):
        """법령 본문 JSON에서 텍스트 내용을 추출하여 포맷팅합니다. (구조 분석 및 예외 처리 강화)"""
        try:
            law_data = json_data.get("법령", json_data)
            text_parts = []

            text_parts.append("="*20 + " 기본 정보 " + "="*20)
            basic_info = law_data.get('기본정보', {})
            if basic_info:
                order = ['법령명_한글', '법종구분', '소관부처', '공포번호', '공포일자', '시행일자', '제개정구분']
                for key in order:
                    if key in basic_info:
                        value = basic_info[key]
                        if isinstance(value, dict):
                            content = value.get('content', '')
                            if content: text_parts.append(f"[{key}] {content.strip()}")
                        elif value and isinstance(value, str):
                            text_parts.append(f"[{key}] {value.strip()}")

            reason_info = law_data.get('제개정이유', {})
            if reason_info and '제개정이유내용' in reason_info:
                text_parts.append("\n" + "="*20 + " 제개정 이유 " + "="*20)
                reason_content = [line.strip() for sublist in reason_info.get('제개정이유내용', []) for line in sublist]
                text_parts.append("\n".join(reason_content))

            revision_info = law_data.get('개정문', {})
            if revision_info and '개정문내용' in revision_info:
                text_parts.append("\n" + "="*20 + " 개정문 " + "="*20)
                revision_content = [line.strip() for sublist in revision_info.get('개정문내용', []) for line in sublist]
                text_parts.append("\n".join(revision_content))

            text_parts.append("\n" + "="*20 + " 조문 정보 " + "="*20)
            article_container = law_data.get('조문')
            articles = []
            if isinstance(article_container, dict):
                articles = article_container.get('조문단위', [])
            elif isinstance(article_container, list):
                articles = article_container

            if not articles:
                text_parts.append("추출된 조문 정보가 없습니다.")
            else:
                for article in articles:
                    article_parts = []
                    clean_content = lambda s: re.sub(r'^\s*<!\[CDATA\[|\]\]>\s*$', '', s.strip())
                    content = clean_content(article.get('조문내용', ''))
                    article_parts.append(content)
                    sub_parts = self._parse_law_article_parts(article)
                    if sub_parts:
                        article_parts.extend(sub_parts)
                    text_parts.append("\n".join(article_parts))

            appendix_info = law_data.get('부칙', {})
            if appendix_info and '부칙단위' in appendix_info:
                text_parts.append("\n" + "="*20 + " 부칙 정보 " + "="*20)
                appendix_units = appendix_info.get('부칙단위', [])
                if not isinstance(appendix_units, list):
                    appendix_units = [appendix_units]
                for appendix_unit in appendix_units:
                    if isinstance(appendix_unit, dict):
                        p_num = appendix_unit.get('부칙공포번호', '')
                        p_date = appendix_unit.get('부칙공포일자', '')
                        text_parts.append(f"\n--- 부칙 (공포번호: {p_num}, 공포일자: {p_date}) ---")
                        raw_content = appendix_unit.get('부칙내용', [])
                        appendix_content_list = []
                        if isinstance(raw_content, list):
                            for item in raw_content:
                                if isinstance(item, list):
                                    appendix_content_list.extend([line.strip() for line in item])
                                elif isinstance(item, str):
                                    appendix_content_list.append(item.strip())
                        elif isinstance(raw_content, str):
                            appendix_content_list.append(raw_content.strip())
                        text_parts.append("\n".join(appendix_content_list))
                    elif isinstance(appendix_unit, str):
                        text_parts.append(f"\n--- 부칙 ---")
                        text_parts.append(appendix_unit.strip())

            return "\n\n".join(text_parts)
        except Exception as e:
            tqdm.write(f" ❌ 법령 본문 JSON 포맷팅 실패: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def _fetch_and_save_law(self, session, law_row, period_output_dir):
        """단일 법령의 본문을 수집하고 텍스트 파일과 JSON 원본 파일로 저장합니다. (JSON 방식)"""
        law_id = law_row['법령ID']
        params = {'OC': self.oc_id, 'target': 'law', 'type': 'JSON', 'ID': law_id}
        
        response_content, _ = await self._make_request(session, 'GET', BASE_URL_SERVICE, params=params)
        if not response_content:
            return {'status': 'DOWNLOAD_FAIL', 'law_info': law_row}

        try:
            json_data = json.loads(response_content)
        except json.JSONDecodeError:
            return {'status': 'PARSE_FAIL', 'law_info': law_row}

        text_content = await self._format_json_to_text(json_data)
        if not text_content:
            return {'status': 'PARSE_FAIL', 'law_info': law_row}

        base_filename = f"{law_row['법령ID']}_{self._sanitize_filename(law_row['법령명한글'])}"
        txt_path = os.path.join(period_output_dir, f"{base_filename}.txt")
        json_path = os.path.join(period_output_dir, f"{base_filename}.json")
        
        try:
            async with aiofiles.open(txt_path, 'w', encoding='utf-8') as f:
                await f.write(text_content)
            
            async with aiofiles.open(json_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(json_data, indent=4, ensure_ascii=False))

        except Exception as e:
            tqdm.write(f"  [실패] ID {law_id}: 파일 저장 중 오류 발생 - {e}")
            return {'status': 'SAVE_FAIL', 'law_info': law_row}
        
        return {'status': 'SUCCESS', 'text': text_content, 'law_info': law_row}

    async def _process_laws_batch(self, session, law_df, period_output_dir):
        """주어진 데이터프레임의 법령들을 수집하고 결과를 반환합니다."""
        if not isinstance(law_df, pd.DataFrame) or law_df.empty: return pd.DataFrame(), {}

        print(f"➡️ {len(law_df)}개 법령의 본문 수집 및 저장 시작...")
        
        tasks = [self._fetch_and_save_law(session, row, period_output_dir) for _, row in law_df.iterrows()]
        results = await tqdm.gather(*tasks, desc="✍️  법령 본문 수집/저장 중")

        status_map = {'SUCCESS': [], 'DOWNLOAD_FAIL': [], 'PARSE_FAIL': [], 'SAVE_FAIL': []}
        all_texts = []
        for res in results:
            status_map.setdefault(res['status'], []).append(res['law_info'])
            all_texts.append(res.get('text'))
        
        df_with_texts = law_df.copy()
        df_with_texts['법령본문'] = all_texts
        
        return df_with_texts, status_map

    async def _write_period_summary(self, period_output_dir, filename_suffix, status_map):
        """수집 기간에 대한 요약 리포트 파일을 작성합니다."""
        success_list = status_map.get('SUCCESS', [])
        skipped_list = status_map.get('SKIPPED_EXISTS', [])
        download_fail_list = status_map.get('DOWNLOAD_FAIL', [])
        parse_fail_list = status_map.get('PARSE_FAIL', [])
        save_fail_list = status_map.get('SAVE_FAIL', [])
        
        total_items = sum(len(v) for v in status_map.values())
        newly_success_count = len(success_list)
        total_success_count = newly_success_count + len(skipped_list)
        success_rate = (total_success_count / total_items * 100) if total_items > 0 else 0
        
        summary_path = os.path.join(period_output_dir, f"crawling_result_{filename_suffix}.txt")
        async with aiofiles.open(summary_path, 'w', encoding='utf-8') as f:
            await f.write(f"--- 수집 결과 요약 ({filename_suffix}) ---\n")
            await f.write(f"총 대상: {total_items}건\n")
            await f.write(f"최종 성공: {total_success_count}건 (신규 수집: {newly_success_count}건, 건너뛰기: {len(skipped_list)}건)\n")
            await f.write(f"수집 실패: {total_items - total_success_count}건\n")
            await f.write(f"성공률: {success_rate:.2f}%\n")
            await f.write("\n--- 세부 내역 ---\n")
            
            if download_fail_list:
                await f.write(f"\n[다운로드 실패: {len(download_fail_list)}건]\n")
                for item in download_fail_list: await f.write(f"- ID: {item['법령ID']}, 법령명: {item['법령명한글']}\n")
            if parse_fail_list:
                await f.write(f"\n[JSON 파싱 실패: {len(parse_fail_list)}건]\n")
                for item in parse_fail_list: await f.write(f"- ID: {item['법령ID']}, 법령명: {item['법령명한글']}\n")
            if save_fail_list:
                await f.write(f"\n[파일 저장 실패: {len(save_fail_list)}건]\n")
                for item in save_fail_list: await f.write(f"- ID: {item['법령ID']}, 법령명: {item['법령명한글']}\n")
        print(f"📄 기간별 리포트가 '{summary_path}'에 저장되었습니다.")

def generate_filename_suffix(**kwargs):
    parts = [f"{k}_{v.replace('.', '-').replace('~', '-')}" for k, v in kwargs.items() if v]
    return "_".join(re.sub(r'[\\/*?:"<>|~]', "_", part) for part in parts)

async def main():
    # 🚨 중요: 여기에 발급받은 인증키를 입력하세요.
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
        scraper = LawScraper(oc_id=MY_OC_ID)

        async def run_collection(params, is_test=False):
            """단일 수집 작업을 실행하고 결과를 반환하는 함수"""
            filename_suffix = generate_filename_suffix(**params)
            period_output_dir = os.path.join(BASE_OUTPUT_DIR, filename_suffix)
            os.makedirs(period_output_dir, exist_ok=True)
            
            print(f"\n" + "="*50 + f"\n▶️ {'테스트' if is_test else '전체'} 수집 실행: {filename_suffix}\n" + "="*50)
            
            list_params = {'display': 5 if is_test else 100, 'max_pages': 1 if is_test else None}
            df_list = await scraper.fetch_law_list(session, **params, **list_params)

            if df_list is None or df_list.empty:
                print(f"\n⚠️ {filename_suffix} 조건에 해당하는 데이터가 없습니다.")
                return None

            print("\n--- 시행일자별 법령 수 ---")
            print(df_list['시행일자'].value_counts())
            print("-------------------------\n")

            print(f"[검증] 기존에 수집된 파일을 확인하여 누락된 항목만 선별합니다...")
            rows_to_collect = []
            rows_to_skip = []
            
            for _, row in df_list.iterrows():
                base_filename = f"{row['법령ID']}_{scraper._sanitize_filename(row['법령명한글'])}"
                txt_path = os.path.join(period_output_dir, f"{base_filename}.txt")
                json_path = os.path.join(period_output_dir, f"{base_filename}.json")
                if await aio_os.path.exists(txt_path) and await aio_os.path.exists(json_path):
                    rows_to_skip.append(row)
                else:
                    rows_to_collect.append(row)
            
            status_map = {'SKIPPED_EXISTS': [dict(row) for row in rows_to_skip]}
            df_final_collected = pd.DataFrame()

            if rows_to_collect:
                df_to_collect = pd.DataFrame(rows_to_collect).reset_index(drop=True)
                df_final_collected, collected_status_map = await scraper._process_laws_batch(session, df_to_collect, period_output_dir)
                for key, value in collected_status_map.items():
                    status_map.setdefault(key, []).extend(value)
            else:
                print(" ✅ 모든 항목이 이미 수집되었습니다. 신규 수집을 건너뜁니다.")

            await scraper._write_period_summary(period_output_dir, filename_suffix, status_map)

            print(f"\n[통합] 기존 파일과 새로 수집된 데이터를 통합합니다...")
            skipped_texts = []
            for row_dict in status_map['SKIPPED_EXISTS']:
                base_filename = f"{row_dict['법령ID']}_{scraper._sanitize_filename(row_dict['법령명한글'])}"
                txt_path = os.path.join(period_output_dir, f"{base_filename}.txt")
                try:
                    async with aiofiles.open(txt_path, 'r', encoding='utf-8') as f:
                        text = await f.read()
                    skipped_texts.append(text)
                except Exception:
                    skipped_texts.append(None)
            
            df_skipped = pd.DataFrame(status_map['SKIPPED_EXISTS'])
            if not df_skipped.empty:
                df_skipped['법령본문'] = skipped_texts
            
            df_final_total = pd.concat([df_final_collected, df_skipped], ignore_index=True)
            if not df_final_total.empty:
                df_final_total = df_final_total.set_index('법령ID').loc[df_list['법령ID'].astype(str)].reset_index()
                json_path = os.path.join(period_output_dir, f"collected_laws_{filename_suffix}.json")
                df_final_total.to_json(json_path, orient='records', force_ascii=False, indent=4)
                print(f"\n💾 최종 통합 결과(JSON)가 '{json_path}'에 저장되었습니다.")
            
            return status_map

        # --- 테스트 케이스 실행 (최신 5건 샘플 수집) ---
        # print("\n" + "="*60 + "\n▶️ 테스트 수집 실행 (최신 5건)\n" + "="*60)
        # test_end_date = pd.to_datetime("2025-07-11")
        # test_start_date = test_end_date - pd.DateOffset(months=1)
        # test_range_str = f"{test_start_date.strftime('%Y%m%d')}~{test_end_date.strftime('%Y%m%d')}"
        # await run_collection({"efyd_range": test_range_str}, is_test=True) 
        # print("\n" + "="*60 + "\n✅ 테스트 수집 완료. 전체 수집을 시작하려면 이 부분을 주석 처리하세요.\n" + "="*60)


        # --- 실제 데이터 수집 로직 (2025.06.01 ~ 2025.06.05) ---
        # 위 테스트가 성공적으로 끝나면, 위 테스트 블록을 주석 처리하고 아래 블록의 주석을 해제하여 전체 수집을 시작하세요.
        
        start_date = pd.to_datetime("2025-06-01") 
        end_date = pd.to_datetime("2025-06-30")   
        print("\n" + "="*60 + f"\n▶️ 실제 법령 데이터 수집 실행 ({start_date.date()} ~ {end_date.date()})\n" + "="*60)
        
        overall_stats = []
        collection_max_retries = 3

        date_ranges = pd.date_range(start=start_date, end=end_date, freq='MS') # MS: Month Start

        for i in range(len(date_ranges)):
            range_start = date_ranges[i]
            range_end = range_start + pd.offsets.MonthEnd(1)
            if range_end > end_date:
                range_end = end_date
            
            date_range_str = f"{range_start.strftime('%Y%m%d')}~{range_end.strftime('%Y%m%d')}"
            
            for attempt in range(collection_max_retries):
                try:
                    status_map = await run_collection({"efyd_range": date_range_str}) 
                    if status_map is not None:
                        overall_stats.append({'period': date_range_str, 'status': 'SUCCESS', 'stats': status_map})
                    else:
                        overall_stats.append({'period': date_range_str, 'status': 'NO_DATA', 'stats': {}})
                    break 
                except Exception as e:
                    print(f"‼️ {date_range_str} 기간 수집 중 심각한 오류 발생 (시도 {attempt + 1}/{collection_max_retries}): {e}")
                    if attempt < collection_max_retries - 1:
                        await asyncio.sleep(5)
                    else:
                        print(f"‼️ {date_range_str} 기간 수집에 최종 실패했습니다.")
                        overall_stats.append({'period': date_range_str, 'status': 'FATAL_ERROR', 'stats': {}})
            
            await _write_overall_summary(overall_stats)
        


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
