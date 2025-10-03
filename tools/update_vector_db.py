import sys
import os
import uuid
from dataclasses import dataclass
from typing import Optional
from qdrant_client.http import models
from tqdm import tqdm
# [MODIFICATION START] 날짜 처리를 위한 datetime, timedelta 임포트
from datetime import datetime, timedelta
# [MODIFICATION END]

# --- 경로 설정 ---
# 스크립트를 직접 실행할 때 'src' 디렉토리를 sys.path에 추가하여
# 'lawdigest_ai'와 'data_operations' 모듈을 찾을 수 있도록 합니다.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from data_operations.DatabaseManager import DatabaseManager
from lawdigest_ai import config as project_config
from lawdigest_ai.embedding_generator import EmbeddingGenerator
from lawdigest_ai.qdrant_manager import QdrantManager

# ===========================================================================
# 설정 영역: 여기서 임베딩 및 메타데이터에 사용할 필드를 관리합니다.
# ===========================================================================
EMBEDDING_FIELDS = [
    {"name": "법안 제목", "key": "bill_name"},
    {"name": "소관 위원회", "key": "committee"},
    {"name": "제안일", "key": "propose_date"},
    {"name": "AI 요약", "key": "gpt_summary"},
    {"name": "한 줄 요약", "key": "brief_summary"},
    {"name": "전체 요약", "key": "summary"},
]
METADATA_FIELDS = [
    "bill_id", "bill_name", "committee", "summary", "brief_summary",
    "gpt_summary", "propose_date", "assembly_number", "stage",
    "bill_result", "proposers"
]

# ===========================================================================
# --- 상수 및 네임스페이스 정의 ---

BATCH_SIZE = 100
NAMESPACE_UUID = uuid.UUID('6f29a8f8-14ca-43a8-8e69-de1a1389c086')

@dataclass
class VectorPipelineConfig:
    """
    Runtime configuration for the Qdrant update pipeline.
    이 클래스의 기본값이 파이프라인의 기본 설정으로 사용됩니다.
    """
    collection_name: str = "KURE_embedding_test"
    recreate: bool = True
    test_mode: bool = True
    batch_size: int = BATCH_SIZE

    # 날짜 필터
    start_date: Optional[str] = '2025-09-17'
    end_date: Optional[str] = '2025-09-17'
    # 날짜 필터가 필요 없을 경우 None으로 설정합니다. (예: '2023-01-01')

    # 임베딩 모델 설정
    model_type = 'huggingface'
    model_name = 'nlpai-lab/KURE-v1'



def get_required_db_fields():
    """설정된 두 리스트를 바탕으로 DB에서 가져와야 할 모든 컬럼명을 계산합니다."""
    embedding_keys = {field['key'] for field in EMBEDDING_FIELDS}
    metadata_keys = set(METADATA_FIELDS)
    all_keys = list(embedding_keys.union(metadata_keys))
    if 'bill_id' not in all_keys:
        all_keys.insert(0, 'bill_id')
    return all_keys

def fetch_bills_from_db(db_manager: DatabaseManager, limit: Optional[int] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """데이터베이스에서 필요한 모든 법안 정보를 동적으로 가져옵니다."""
    # [MODIFICATION START] 함수 호출 시 단계 표시 메시지를 run_pipeline으로 이동
    # print("\n-- [단계 1/3] 데이터베이스에서 법안 데이터 조회 --")
    # [MODIFICATION END]
    
    fields_to_fetch = get_required_db_fields()
    query = f"SELECT {', '.join(fields_to_fetch)} FROM Bill"
    
    where_clauses = []
    params = []

    if start_date:
        where_clauses.append("propose_date >= %s")
        params.append(start_date)
        # [MODIFICATION START] 필터 로그 메시지를 run_pipeline으로 이동
        # print(f"▶️ 시작일 필터: {start_date}")
        # [MODIFICATION END]
    
    if end_date:
        where_clauses.append("propose_date <= %s")
        params.append(end_date)
        # [MODIFICATION START] 필터 로그 메시지를 run_pipeline으로 이동
        # print(f"▶️ 종료일 필터: {end_date}")
        # [MODIFICATION END]

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    if limit:
        query += f" LIMIT {limit}"
    query += ";"

    try:
        # [MODIFICATION START] 쿼리 실행 로그를 run_pipeline으로 이동
        # print(f"▶️ 필요한 필드 목록: {fields_to_fetch}")
        # print(f"▶️ 실행 쿼리: {query}")
        # [MODIFICATION END]
        bills = db_manager.execute_query(query, tuple(params) if params else None)
        # [MODIFICATION START] 결과 로그를 run_pipeline으로 이동
        # print(f"✅ 총 {len(bills)}개의 법안 데이터를 성공적으로 조회했습니다.")
        # [MODIFICATION END]
        return bills
    except Exception as e:
        print(f"❌ 데이터베이스에서 법안 조회 중 오류 발생: {e}")
        return []


def run_pipeline(pipeline_config: VectorPipelineConfig):
    """
    전체 데이터 파이프라인을 실행합니다.

    Args:
        pipeline_config (VectorPipelineConfig): 실행에 필요한 파라미터를 포함한 설정 객체.
    """
    if pipeline_config.test_mode:
        print("\n🧪 테스트 모드로 실행합니다. 5개의 데이터만 처리합니다.")

    print(f"🚀 Qdrant 컬렉션 '{pipeline_config.collection_name}'에 대한 파이프라인을 시작합니다.")

    # [MODIFICATION START] 날짜 유효성 검사 로직 추가
    if pipeline_config.start_date and pipeline_config.end_date:
        start_dt = datetime.strptime(pipeline_config.start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(pipeline_config.end_date, '%Y-%m-%d')
        if start_dt > end_dt:
            print(f"❌ 설정 오류: 시작일({pipeline_config.start_date})이 종료일({pipeline_config.end_date})보다 늦을 수 없습니다.")
            return
    # [MODIFICATION END]

    try:
        project_config.validate_config()
    except ValueError as e:
        print(f"❌ 설정 오류: {e}")
        return

    db_manager = DatabaseManager()
    embed_generator = EmbeddingGenerator(model_type=pipeline_config.model_type, model_name=pipeline_config.model_name)
    qdrant_manager = QdrantManager()

    # --- 객체 초기화 상태 디버깅 ---
    db_status = db_manager.connection is not None
    qdrant_status = qdrant_manager.client is not None
    embed_status = (embed_generator.model_type == 'openai' and embed_generator.client is not None) or \
                   (embed_generator.model_type == 'huggingface' and embed_generator.huggingface_model is not None)

    if not all([db_status, qdrant_status, embed_status]):
        print("❌ 파이프라인 실행에 필요한 객체 초기화에 실패했습니다. 작업을 중단합니다.")
        print(f"  - DB 연결 상태: {'성공' if db_status else '실패'}")
        print(f"  - 임베딩 생성기 상태: {'성공' if embed_status else '실패'} (모델 타입: {embed_generator.model_type})")
        print(f"  - Qdrant 클라이언트 상태: {'성공' if qdrant_status else '실패'}")
        return

    # --- 벡터 차원 동적 결정 ---
    vector_size = 0
    if embed_generator.model_type == 'openai':
        # OpenAI 모델의 경우, 테스트 임베딩을 생성하여 차원을 확인
        print("🧪 OpenAI 모델의 벡터 차원을 확인하기 위해 테스트 임베딩을 생성합니다...")
        dummy_vector = embed_generator.generate("test")
        if dummy_vector:
            vector_size = len(dummy_vector)
    elif embed_generator.model_type == 'huggingface' and embed_generator.huggingface_model:
        vector_size = embed_generator.huggingface_model.get_sentence_embedding_dimension()

    if not vector_size:
        print("❌ 임베딩 벡터의 차원을 결정할 수 없어 파이프라인을 중단합니다.")
        return
    
    print(f"✅ 동적으로 확인된 벡터 차원: {vector_size}")

    qdrant_manager.create_collection(
        collection_name=pipeline_config.collection_name,
        vector_size=vector_size,
        recreate=pipeline_config.recreate,
    )

    limit = 5 if pipeline_config.test_mode else None
    
    # [MODIFICATION START] 일별 순차 조회 로직으로 변경
    print("\n-- [단계 1/3] 데이터베이스에서 법안 데이터 조회 --")
    all_bills = []
    if pipeline_config.start_date and pipeline_config.end_date:
        start_dt = datetime.strptime(pipeline_config.start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(pipeline_config.end_date, '%Y-%m-%d')
        current_dt = start_dt
        
        print(f"▶️ 조회 기간: {pipeline_config.start_date} ~ {pipeline_config.end_date}")
        
        while current_dt <= end_dt:
            current_date_str = current_dt.strftime('%Y-%m-%d')
            print(f"⏳ {current_date_str} 데이터 조회 중...")
            # 하루치 데이터만 조회
            daily_bills = fetch_bills_from_db(
                db_manager, 
                limit=limit, 
                start_date=current_date_str, 
                end_date=current_date_str
            )
            if daily_bills:
                all_bills.extend(daily_bills)
            
            current_dt += timedelta(days=1)
            # 테스트 모드에서는 하루만 실행하고 종료
            if pipeline_config.test_mode:
                break
    else:
        # 날짜 지정이 없으면 전체 데이터를 가져옴
        print("▶️ 전체 기간의 데이터를 조회합니다.")
        all_bills = fetch_bills_from_db(db_manager, limit=limit)
    
    print(f"✅ 총 {len(all_bills)}개의 법안 데이터를 성공적으로 조회했습니다.")
    bills = all_bills
    # [MODIFICATION END]
    
    if not bills:
        print("⚠️ 처리할 법안 데이터가 없습니다. 작업을 종료합니다.")
        db_manager.close()
        return

    print(
        "\n-- [단계 2/3] 텍스트 임베딩 생성 및 Qdrant 업서트 (배치 크기: "
        f"{pipeline_config.batch_size}) --"
    )
    points_batch = []
    for bill in tqdm(bills, desc="임베딩 생성 및 업서트 처리 중"):
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
            
            qdrant_id = str(uuid.uuid5(NAMESPACE_UUID, bill['bill_id']))

            point = models.PointStruct(
                id=qdrant_id,
                vector=vector,
                payload=payload
            )
            points_batch.append(point)
        
        if len(points_batch) >= pipeline_config.batch_size:
            if points_batch:
                qdrant_manager.upsert_points(
                    collection_name=pipeline_config.collection_name,
                    points=points_batch,
                )
                points_batch = []

    if points_batch:
        qdrant_manager.upsert_points(
            collection_name=pipeline_config.collection_name,
            points=points_batch,
        )

    print("\n-- [단계 3/3] 작업 완료 및 자원 해제 --")
    db_manager.close()
    print("🎉 모든 작업이 성공적으로 완료되었습니다.")

def main():
    """
    스크립트의 메인 실행 함수입니다.
    VectorPipelineConfig 클래스에 정의된 기본값을 사용하여 파이프라인을 실행합니다.
    """
    pipeline_config = VectorPipelineConfig()

    if not pipeline_config.collection_name:
        raise ValueError("Qdrant 컬렉션 이름이 지정되지 않았습니다. VectorPipelineConfig 클래스에서 기본값을 설정하세요.")

    run_pipeline(pipeline_config)


# 스크립트가 직접 실행될 때 main 함수를 호출합니다.
if __name__ == "__main__":
    main()

