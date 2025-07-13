import pytest
import os
import pandas as pd
from dotenv import load_dotenv

# 테스트 대상 모듈 import
import data_operations as dataops
from data_operations import AISummarizer

# --- Pytest Hooks: 커스텀 옵션 추가 ---

def pytest_addoption(parser):
    """
    pytest에 커스텀 커맨드라인 옵션을 추가하는 hook 함수입니다.
    --rows 옵션을 추가하여 AI 요약을 수행할 행 개수를 지정할 수 있습니다.
    """
    parser.addoption(
        "--rows", action="store", default=3, type=int, help="AI 요약을 수행할 행 개수를 지정합니다. (기본값: 3)"
    )


# --- Fixtures: 테스트 환경 및 데이터 준비 ---

@pytest.fixture(scope="session", autouse=True)
def check_env_vars():
    """
    테스트 세션 시작 시 .env 파일을 로드하고 필수 환경 변수가 있는지 확인합니다.
    autouse=True로 설정하여 자동으로 실행됩니다.
    """
    load_dotenv()
    if not os.getenv("APIKEY_OPENAI"):
        pytest.fail("'.env' 파일에 'APIKEY_OPENAI' 환경 변수가 설정되지 않았습니다. 테스트를 진행할 수 없습니다.")

@pytest.fixture(scope="module")
def params_ai():
    """테스트에 사용할 파라미터를 반환합니다."""
    # 실제 데이터가 있을 법한 날짜로 설정 (필요시 수정)
    return {
        'start_date': '2024-06-01',
        'end_date': '2024-06-01'
    }

@pytest.fixture(scope="module")
def raw_bills_df(params_ai):
    """
    DataFetcher를 사용하여 원본 법안 데이터를 가져옵니다.
    데이터가 없으면 테스트를 건너뜁니다.
    scope='module'로 설정하여 모듈 내 테스트들에서 한 번만 실행됩니다.
    """
    try:
        fetcher = dataops.DataFetcher(params_ai)
        df = fetcher.fetch_data('bills')
        if df.empty:
            pytest.skip(f"해당 기간({params_ai['start_date']})에 데이터가 없어 통합 테스트를 건너뜁니다.")
        return df
    except Exception as e:
        pytest.fail(f"데이터를 가져오는 중 오류가 발생했습니다: {e}\n'data_operations.py'를 확인해주세요.")

@pytest.fixture
def df_for_test(raw_bills_df, request):
    """
    실제 테스트에 사용할 데이터프레임을 준비합니다.
    --rows 옵션으로 지정된 수만큼의 데이터만 사용하고, 요약 컬럼을 초기화합니다.
    """
    # --rows 옵션에서 값을 가져옵니다.
    num_rows = request.config.getoption("--rows")
    print(f"\n--rows 옵션에 따라 상위 {num_rows}개의 데이터로 테스트를 진행합니다.")
    
    # 지정된 행 개수만큼 데이터 선택
    df_sample = raw_bills_df.head(num_rows).copy()
    
    if df_sample.empty:
        pytest.skip(f"테스트할 데이터가 없습니다. (요청: {num_rows}개)")
    
    # 테스트를 위해 항상 새로운 요약을 생성하도록 기존 요약 내용을 초기화
    df_sample['briefSummary'] = None
    df_sample['gptSummary'] = None
    
    return df_sample


# --- Test Function: 실제 API를 호출하는 통합 테스트 ---

def test_ai_summarizer_end_to_end(df_for_test):
    """
    AISummarizer의 제목 및 내용 요약 기능을 End-to-End로 테스트합니다.
    실제 데이터를 가져와 OpenAI API를 호출하고, 결과가 올바르게 DataFrame에
    저장되는지 검증합니다.
    """
    # Given: AISummarizer 인스턴스와 테스트 데이터 준비
    summarizer = AISummarizer()
    initial_rows = len(df_for_test)
    print(f"\n{initial_rows}건의 법안에 대한 통합 테스트 시작...")

    # When: 제목 요약 및 내용 요약 함수를 순차적으로 실행
    df_with_titles = summarizer.AI_title_summarize(df_for_test)
    df_final = summarizer.AI_content_summarize(df_with_titles)

    # Then: 결과 검증
    print("\n--- 테스트 결과 검증 ---")
    print("요약 후 데이터 샘플:")
    print(df_final[['billName', 'briefSummary', 'gptSummary']])

    # 1. 최종 데이터프레임의 행 수가 초기와 동일한지 확인
    assert len(df_final) == initial_rows, "처리 후 데이터의 행 수가 변경되었습니다."

    # 2. 'briefSummary' 컬럼에 None이나 NaN 값이 없는지 확인
    assert df_final['briefSummary'].notnull().all(), \
        "'briefSummary' 컬럼에 요약되지 않은 항목이 있습니다."

    # 3. 'gptSummary' 컬럼에 None이나 NaN 값이 없는지 확인
    assert df_final['gptSummary'].notnull().all(), \
        "'gptSummary' 컬럼에 요약되지 않은 항목이 있습니다."
        
    # 4. 각 요약 결과가 비어있지 않은 문자열인지 첫 번째 행을 샘플로 확인
    assert isinstance(df_final.iloc[0]['briefSummary'], str) and len(df_final.iloc[0]['briefSummary']) > 0
    assert isinstance(df_final.iloc[0]['gptSummary'], str) and len(df_final.iloc[0]['gptSummary']) > 0

    print("\n모든 검증 통과. 통합 테스트 성공!")
