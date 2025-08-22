import sys
import os
import argparse
import uuid
from qdrant_client.http import models

# --- 경로 설정 ---
# 현재 스크립트(tools)의 상위 디렉토리인 프로젝트 루트를 시스템 경로에 추가합니다.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- 모듈 임포트 ---
# 이제 src 패키지에서 필요한 모듈을 절대 경로로 임포트할 수 있습니다.
from src.data_operations.DatabaseManager import DatabaseManager
from src.lawdigest_ai import config
from src.lawdigest_ai.embedding_generator import EmbeddingGenerator
from src.lawdigest_ai.qdrant_manager import QdrantManager

# ===========================================================================
# 설정 영역: 여기서 임베딩 및 메타데이터에 사용할 필드를 관리합니다.
# ===========================================================================

# 임베딩에 사용할 텍스트 필드 목록
EMBEDDING_FIELDS = [
    {"name": "법안 제목", "key": "bill_name"},
    {"name": "소관 위원회", "key": "committee"},
    {"name": "제안일", "key": "propose_date"},
    {"name": "AI 요약", "key": "gpt_summary"},
    {"name": "한 줄 요약", "key": "brief_summary"},
    {"name": "전체 요약", "key": "summary"},
]

# Qdrant 페이로드에 저장할 메타데이터 필드 목록 (DB 컬럼명)
METADATA_FIELDS = [
    "bill_id", "bill_name", "committee", "summary", "brief_summary",
    "gpt_summary", "propose_date", "assembly_number", "stage",
    "bill_result", "proposers"
]

# ===========================================================================

# --- 상수 및 네임스페이스 정의 ---
VECTOR_SIZE = 1536
BATCH_SIZE = 100
# bill_id로부터 일관된 UUID를 생성하기 위한 네임스페이스
# 이 값은 절대 변경되면 안 됩니다.
NAMESPACE_UUID = uuid.UUID('6f29a8f8-14ca-43a8-8e69-de1a1389c086')

def get_required_db_fields():
    """설정된 두 리스트를 바탕으로 DB에서 가져와야 할 모든 컬럼명을 계산합니다."""
    embedding_keys = {field['key'] for field in EMBEDDING_FIELDS}
    metadata_keys = set(METADATA_FIELDS)
    all_keys = list(embedding_keys.union(metadata_keys))
    if 'bill_id' not in all_keys:
        all_keys.insert(0, 'bill_id')
    return all_keys

def fetch_bills_from_db(db_manager: DatabaseManager, limit: int = None):
    """데이터베이스에서 필요한 모든 법안 정보를 동적으로 가져옵니다."""
    print("\n-- [단계 1/3] 데이터베이스에서 법안 데이터 조회 --")
    
    fields_to_fetch = get_required_db_fields()
    query = f"SELECT { ', '.join(fields_to_fetch) } FROM Bill"
    
    if limit:
        query += f" LIMIT {limit}"
    query += ";"

    try:
        print(f"▶️ 필요한 필드 목록: {fields_to_fetch}")
        print(f"▶️ 실행 쿼리: {query}")
        bills = db_manager.execute_query(query)
        print(f"✅ 총 {len(bills)}개의 법안 데이터를 성공적으로 조회했습니다.")
        return bills
    except Exception as e:
        print(f"❌ 데이터베이스에서 법안 조회 중 오류 발생: {e}")
        return []

def run_pipeline(collection_name: str, recreate: bool = False, test_mode: bool = False):
    """
    전체 데이터 파이프라인을 실행합니다.

    Args:
        collection_name (str): 데이터를 저장할 Qdrant 컬렉션 이름.
        recreate (bool): True이면 컬렉션을 강제로 재생성합니다.
        test_mode (bool): True이면 5개의 데이터만으로 테스트를 실행합니다.
    """
    if test_mode:
        print("\n🧪 테스트 모드로 실행합니다. 5개의 데이터만 처리합니다.")
    
    print(f"🚀 Qdrant 컬렉션 '{collection_name}'에 대한 파이프라인을 시작합니다.")
    
    try:
        config.validate_config()
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        return

    db_manager = DatabaseManager()
    embed_generator = EmbeddingGenerator()
    qdrant_manager = QdrantManager()

    if not all([db_manager.connection, embed_generator.client, qdrant_manager.client]):
        print("❌ 파이프라인 실행에 필요한 객체 초기화에 실패했습니다. 작업을 중단합니다.")
        return

    qdrant_manager.create_collection(collection_name=collection_name, vector_size=VECTOR_SIZE, recreate=recreate)
    
    limit = 5 if test_mode else None
    bills = fetch_bills_from_db(db_manager, limit=limit)
    
    if not bills:
        print("⚠️ 처리할 법안 데이터가 없습니다. 작업을 종료합니다.")
        db_manager.close()
        return

    print(f"\n-- [단계 2/3] 텍스트 임베딩 생성 및 Qdrant 업서트 (배치 크기: {BATCH_SIZE}) --")
    points_batch = []
    for i, bill in enumerate(bills):
        text_parts = []
        for field in EMBEDDING_FIELDS:
            value = bill.get(field['key'])
            if value:
                value_str = value.strftime('%Y-%m-%d') if hasattr(value, 'strftime') else str(value)
                text_parts.append(f"{field['name']}: {value_str}")
        text_to_embed = "\n\n".join(text_parts)

        vector = embed_generator.generate(text_to_embed)

        if vector:
            payload = {}
            for key in METADATA_FIELDS:
                value = bill.get(key)
                if value is not None:
                    payload[key] = value.isoformat() if hasattr(value, 'isoformat') else value
            
            # bill_id를 기반으로 결정론적 UUID 생성
            qdrant_id = str(uuid.uuid5(NAMESPACE_UUID, bill['bill_id']))

            point = models.PointStruct(
                id=qdrant_id, # UUID를 포인트 ID로 사용
                vector=vector,
                payload=payload
            )
            points_batch.append(point)

        if (i + 1) % 100 == 0 and not test_mode:
            print(f"⏳ ({i + 1}/{len(bills)})개 법안 처리 완료...")

        if len(points_batch) >= BATCH_SIZE or (i + 1) == len(bills):
            if points_batch:
                qdrant_manager.upsert_points(collection_name=collection_name, points=points_batch)
                points_batch = []

    print("\n-- [단계 3/3] 작업 완료 및 자원 해제 --")
    db_manager.close()
    print("🎉 모든 작업이 성공적으로 완료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="법안 데이터를 DB에서 읽어와 Qdrant 벡터 DB에 업로드하는 파이프라인")
    parser.add_argument("-c", "--collection", type=str, required=True, help="데이터를 저장할 Qdrant 컬렉션 이름")
    parser.add_argument("-r", "--recreate", action='store_true', help="이 플래그가 있으면 컬렉션을 강제로 재생성합니다.")
    parser.add_argument("-t", "--test", action='store_true', help="테스트 모드로 실행 (5개 데이터만 처리)")
    
    args = parser.parse_args()
    
    run_pipeline(
        collection_name=args.collection,
        recreate=args.recreate,
        test_mode=args.test
    )