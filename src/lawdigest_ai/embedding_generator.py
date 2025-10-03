import sys
import os

# 스크립트를 직접 실행할 수 있도록 sys.path를 동적으로 수정합니다.
# 현재 파일의 상위 디렉토리(src)를 sys.path에 추가합니다.
if __package__ is None or __package__ == '':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(script_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from openai import OpenAI
from sentence_transformers import SentenceTransformer
from lawdigest_ai import config

class EmbeddingGenerator:
    """
    OpenAI 또는 HuggingFace 임베딩 모델을 사용하여 주어진 텍스트로부터 벡터 표현(임베딩)을 생성하는 역할을 합니다.
    """
    def __init__(self, model_type='openai', model_name=None):
        """
        EmbeddingGenerator 클래스의 인스턴스를 생성할 때 호출됩니다.

        Args:
            model_type (str): 사용할 모델 유형 ('openai' 또는 'huggingface').
            model_name (str): HuggingFace 모델을 사용할 경우, 모델의 이름.
        """
        self.model_type = model_type
        self.client = None
        self.huggingface_model = None

        if model_type == 'openai':
            try:
                # OpenAI API 키를 사용하여 클라이언트 객체를 생성합니다.
                self.client = OpenAI(api_key=config.OPENAI_API_KEY)
                print("✅ OpenAI 클라이언트가 성공적으로 초기화되었습니다.")
            except Exception as e:
                # 초기화 과정에서 오류 발생 시, 에러 메시지를 출력합니다.
                print(f"❌ OpenAI 클라이언트 초기화에 실패했습니다: {e}")
        elif model_type == 'huggingface':
            if not model_name:
                print("❌ HuggingFace 모델을 사용하려면 model_name을 지정해야 합니다.")
                return
            try:
                # 지정된 HuggingFace 모델을 로드합니다.
                self.huggingface_model = SentenceTransformer(model_name)
                print(f"✅ HuggingFace 모델 '{model_name}'이(가) 성공적으로 로드되었습니다.")
            except Exception as e:
                # 모델 로딩 과정에서 오류 발생 시, 에러 메시지를 출력합니다.
                print(f"❌ HuggingFace 모델 로드에 실패했습니다: {e}")
        else:
            print(f"❌ 지원하지 않는 모델 유형입니다: {model_type}")

    def generate(self, text: str):
        """
        입력된 텍스트 한 조각에 대한 임베딩 벡터를 생성합니다.

        Args:
            text (str): 임베딩을 생성할 대상 텍스트입니다.

        Returns:
            list[float] or None: 성공 시, 텍스트에 해당하는 임베딩 벡터(실수 리스트)를 반환합니다. 
                                 실패 시, None을 반환합니다.
        """
        # 입력된 텍스트가 유효한지(비어있지 않고, 문자열 타입인지) 확인합니다.
        if not text or not isinstance(text, str):
            print("⚠️ 임베딩할 텍스트가 유효하지 않습니다. (빈 문자열 또는 None)")
            return None

        if self.model_type == 'openai':
            # OpenAI 클라이언트가 정상적으로 초기화되었는지 확인합니다.
            if not self.client:
                print("❌ OpenAI 클라이언트가 초기화되지 않아 임베딩을 생성할 수 없습니다.")
                return None
            try:
                # OpenAI의 embeddings.create API를 호출하여 임베딩을 요청합니다.
                response = self.client.embeddings.create(
                    input=[text.replace("\n", " ")],  # API는 텍스트 내 개행 문자를 공백으로 처리하는 것을 권장합니다.
                    model=config.EMBEDDING_MODEL
                )
                # API 응답 구조에 따라 결과 데이터에서 임베딩 벡터를 추출합니다.
                embedding = response.data[0].embedding
                return embedding
            except Exception as e:
                # API 호출 중 예외가 발생하면 오류 메시지를 출력하고 None을 반환합니다.
                print(f"❌ OpenAI 텍스트 임베딩 생성 중 오류가 발생했습니다: {e}")
                return None
        elif self.model_type == 'huggingface':
            # HuggingFace 모델이 정상적으로 로드되었는지 확인합니다.
            if not self.huggingface_model:
                print("❌ HuggingFace 모델이 로드되지 않아 임베딩을 생성할 수 없습니다.")
                return None
            try:
                # HuggingFace 모델을 사용하여 임베딩을 생성합니다.
                embedding = self.huggingface_model.encode(text)
                return embedding.tolist()
            except Exception as e:
                # 임베딩 생성 중 예외가 발생하면 오류 메시지를 출력하고 None을 반환합니다.
                print(f"❌ HuggingFace 텍스트 임베딩 생성 중 오류가 발생했습니다: {e}")
                return None
        else:
            print(f"❌ 지원하지 않는 모델 유형({self.model_type})으로는 임베딩을 생성할 수 없습니다.")
            return None

if __name__ == '__main__':
    # --- 테스트 설정 ---
    # 테스트할 모델 타입을 'openai' 또는 'huggingface'로 설정하세요.
    TEST_MODEL_TYPE = 'huggingface'
    # HuggingFace 모델을 테스트할 경우, 모델 이름을 지정하세요.
    TEST_MODEL_NAME = 'nlpai-lab/KURE-v1'
    # 임베딩을 생성할 텍스트
    TEST_TEXT = "대한민국 헌법 제1조 1항: 대한민국은 민주공화국이다."
    # --- 테스트 설정 끝 ---

    print(f"\n--- 임베딩 생성 테스트({TEST_MODEL_TYPE}) ---")

    generator = EmbeddingGenerator(model_type=TEST_MODEL_TYPE, model_name=TEST_MODEL_NAME)

    is_ready = (TEST_MODEL_TYPE == 'openai' and generator.client) or \
               (TEST_MODEL_TYPE == 'huggingface' and generator.huggingface_model)

    if is_ready:
        print(f"입력 텍스트: '{TEST_TEXT}'")
        embedding = generator.generate(TEST_TEXT)
        if embedding:
            print(f"✅ 임베딩 생성 성공!")
            print(f"   - 벡터 차원: {len(embedding)}")
            print(f"   - 벡터 일부: {embedding[:5]}...")
        else:
            print("❌ 임베딩 생성에 실패했습니다.")
    else:
        print(f"❌ 모델({TEST_MODEL_TYPE})이 준비되지 않아 테스트를 진행할 수 없습니다.")

    print("\n--- 테스트 완료 ---")