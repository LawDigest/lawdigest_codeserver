import qdrant_client
from qdrant_client.http import models
from . import config

class QdrantManager:
    """
    Qdrant 벡터 데이터베이스와의 연결 및 상호작용을 관리하는 클래스입니다.
    """
    def __init__(self):
        """
        QdrantManager 클래스의 인스턴스를 생성할 때 호출됩니다.
        config.py에 정의된 호스트, API 키, HTTPS 사용 여부 정보를 사용하여 Qdrant 클라이언트를 초기화합니다.
        """
        try:
            self.client = qdrant_client.QdrantClient(
                host=config.QDRANT_HOST, 
                api_key=config.QDRANT_API_KEY,
                port=6333,  # Qdrant의 기본 gRPC 포트
                https=config.QDRANT_USE_HTTPS # HTTPS 사용 여부를 config에서 가져와 설정
            )
            protocol = "https" if config.QDRANT_USE_HTTPS else "http"
            print(f"✅ Qdrant 클라이언트가 성공적으로 초기화되었습니다. ({protocol}://{config.QDRANT_HOST}:{6333})")
        except Exception as e:
            print(f"❌ Qdrant 클라이언트 초기화에 실패했습니다: {e}")
            self.client = None

    def create_collection(self, collection_name: str, vector_size: int, recreate: bool = False):
        """
        Qdrant에 컬렉션을 생성하거나 재생성합니다.

        Args:
            collection_name (str): 생성할 컬렉션의 이름.
            vector_size (int): 컬렉션에 저장될 임베딩 벡터의 차원 수.
            recreate (bool): True일 경우, 동일한 이름의 컬렉션이 이미 존재하면 삭제 후 재생성합니다.
        """
        if not self.client:
            print("❌ Qdrant 클라이언트가 초기화되지 않아 컬렉션을 생성할 수 없습니다.")
            return

        try:
            # recreate 플래그가 True이면, 묻지도 따지지도 않고 컬렉션을 재생성합니다.
            if recreate:
                print(f"⚠️ --recreate 플래그가 활성화되었습니다. 기존 컬렉션 '{collection_name}'을(를) 삭제하고 재생성합니다.")
                self.client.recreate_collection(
                    collection_name=collection_name,
                    vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE)
                )
                print(f"✅ 컬렉션 '{collection_name}'이(가) 성공적으로 재생성되었습니다.")
                return

            # recreate 플래그가 False일 경우, 컬렉션 존재 여부를 확인합니다.
            collections_response = self.client.get_collections()
            existing_collections = [collection.name for collection in collections_response.collections]
            
            if collection_name not in existing_collections:
                print(f"ℹ️ 컬렉션 '{collection_name}'이(가) 존재하지 않습니다. 새로 생성합니다.")
                self.client.recreate_collection(
                    collection_name=collection_name,
                    vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE)
                )
                print(f"✅ 컬렉션 '{collection_name}'이(가) 성공적으로 생성되었습니다.")
            else:
                print(f"✅ 컬렉션 '{collection_name}'이(가) 이미 존재합니다. (재생성하려면 --recreate 옵션을 사용하세요)")
        except Exception as e:
            print(f"❌ 컬렉션 작업 중 오류가 발생했습니다: {e}")

    def upsert_points(self, collection_name: str, points: list):
        """
        데이터 포인트(PointStruct) 리스트를 지정된 Qdrant 컬렉션에 업서트합니다.

        Args:
            collection_name (str): 데이터를 업서트할 컬렉션의 이름.
            points (list[models.PointStruct]): Qdrant에 저장할 데이터 포인트 객체의 리스트.
        """
        if not self.client:
            print("❌ Qdrant 클라이언트가 초기화되지 않아 데이터를 업로드할 수 없습니다.")
            return
        
        if not points:
            print("⚠️ 업서트할 데이터가 없습니다.")
            return

        try:
            # 지정된 컬렉션에 포인트들을 업서트합니다.
            self.client.upsert(
                collection_name=collection_name,
                points=points,
                wait=True  # API 호출이 완전히 처리될 때까지 대기하여 작업의 성공을 보장합니다.
            )
            print(f"✅ {len(points)}개의 데이터 포인트를 '{collection_name}' 컬렉션에 성공적으로 업서트했습니다.")
        except Exception as e:
            print(f"❌ 데이터 업서트 중 오류가 발생했습니다: {e}")
