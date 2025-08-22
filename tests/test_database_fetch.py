import sys
import os
import json

# --- 경로 설정 ---
# src.data_operations.DatabaseManager를 임포트하기 위해 프로젝트 루트를 경로에 추가합니다.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_operations.DatabaseManager import DatabaseManager

def test_fetch_latest_bills():
    """
    데이터베이스에 연결하여 발의 날짜(propose_date)를 기준으로
    가장 최신 법안 5개를 조회하여 출력하는 테스트 함수입니다.
    """
    print("🚀 데이터베이스 연결 및 최신 법안 조회 테스트를 시작합니다...")
    
    db_manager = None
    try:
        # 1. DatabaseManager 객체를 생성하여 데이터베이스에 연결합니다.
        # .env 파일에 설정된 접속 정보를 사용합니다.
        db_manager = DatabaseManager()

        # 연결 실패 시, 함수를 종료합니다.
        if not db_manager.connection:
            print("❌ 데이터베이스 연결에 실패하여 테스트를 중단합니다.")
            return

        # 2. 실행할 SQL 쿼리를 정의합니다.
        # Bill 테이블에서 모든 컬럼을 선택하고, propose_date를 기준으로 내림차순 정렬한 후,
        # 상위 5개만 가져옵니다.
        query = "SELECT * FROM Bill ORDER BY propose_date DESC LIMIT 5;"
        
        print(f"\n▶️ 실행 쿼리:\n{query}\n")

        # 3. 쿼리를 실행하여 결과를 가져옵니다.
        latest_bills = db_manager.execute_query(query)

        # 4. 결과를 확인하고 출력합니다.
        if latest_bills:
            print("✅ 쿼리 성공! 최신 법안 5개를 가져왔습니다.")
            print("--- 조회 결과 ---")
            # 각 법안 정보를 보기 쉽게 JSON 형태로 변환하여 출력합니다.
            for bill in latest_bills:
                # datetime, date 객체는 JSON으로 바로 변환되지 않으므로 문자열로 바꿔줍니다.
                for key, value in bill.items():
                    if hasattr(value, 'isoformat'):
                        bill[key] = value.isoformat()
                print(json.dumps(bill, indent=2, ensure_ascii=False))
                print("---")
        else:
            print("⚠️ 조회된 데이터가 없습니다. 테이블이 비어있거나 쿼리에 문제가 있을 수 있습니다.")

    except Exception as e:
        print(f"❌ 테스트 중 오류가 발생했습니다: {e}")
    finally:
        # 5. 데이터베이스 연결을 종료합니다.
        if db_manager:
            db_manager.close()

if __name__ == "__main__":
    # 이 스크립트가 직접 실행될 때 테스트 함수를 호출합니다.
    test_fetch_latest_bills()
