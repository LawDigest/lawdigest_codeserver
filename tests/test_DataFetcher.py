import os
import pytest
import pandas as pd
from data_operations import DataFetcher
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture
def params():
    return {
        "start_date": "2025-06-01",
        "end_date": "2025-07-01",
        "start_ord": "22",
        "end_ord": "22"
    }

def test_fetch_bills_content(params):
    """✅ 실제 API 호출 테스트: 의안 주요내용 수집"""
    print("\n🔍 테스트 시작: 의안 주요내용 API 호출")

    # API 키 존재 여부 확인
    assert os.environ.get("APIKEY_billsContent"), "❌ APIKEY_billsContent가 설정되어 있지 않습니다."
    print(f"✅ APIKEY_billsContent 존재 확인")

    # 데이터 수집
    fetcher = DataFetcher(params=params, subject="bill_content")
    df = fetcher.content

    print(f"📊 수집된 데이터 개수: {len(df)}개")

    # 기본 검증
    assert isinstance(df, pd.DataFrame), "❌ 수집된 결과가 DataFrame이 아닙니다."
    assert not df.empty, "❌ API 응답 결과가 비어 있습니다."
    assert "summary" in df.columns, "❌ 'summary' 컬럼이 존재하지 않습니다."
    assert df["summary"].notna().all(), "❌ 'summary' 컬럼에 결측치가 존재합니다."

    # 샘플 데이터 출력
    print("\n📌 수집된 데이터 샘플:")
    print(df.head(3).to_string(index=False))

    # 컬럼 목록 확인
    print("\n📑 수집된 컬럼 목록:")
    print(df.columns.tolist())

    # 날짜별 법안 수 확인
    if "proposeDate" in df.columns:
        print("\n📅 날짜별 수집 건수:")
        print(df["proposeDate"].value_counts().sort_index().to_string())

    print("\n✅ 테스트 통과: 실제 API를 통해 데이터가 성공적으로 수집되고 검증되었습니다.")