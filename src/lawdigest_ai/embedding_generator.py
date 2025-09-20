from openai import OpenAI
from . import config

class EmbeddingGenerator:
    """
    OpenAI의 임베딩 모델을 사용하여 주어진 텍스트로부터 벡터 표현(임베딩)을 생성하는 역할을 합니다.
    """
    def __init__(self):
        """
        EmbeddingGenerator 클래스의 인스턴스를 생성할 때 호출됩니다.
        config.py에 정의된 API 키를 사용하여 OpenAI 클라이언트를 초기화합니다.
        """
        try:
            # OpenAI API 키를 사용하여 클라이언트 객체를 생성합니다.
            self.client = OpenAI(api_key=config.OPENAI_API_KEY)
            print("✅ OpenAI 클라이언트가 성공적으로 초기화되었습니다.")
        except Exception as e:
            # 초기화 과정에서 오류 발생 시, 에러 메시지를 출력하고 클라이언트를 None으로 설정합니다.
            print(f"❌ OpenAI 클라이언트 초기화에 실패했습니다: {e}")
            self.client = None

    def generate(self, text: str):
        """
        입력된 텍스트 한 조각에 대한 임베딩 벡터를 생성합니다.

        Args:
            text (str): 임베딩을 생성할 대상 텍스트입니다.

        Returns:
            list[float] or None: 성공 시, 텍스트에 해당하는 임베딩 벡터(실수 리스트)를 반환합니다. 
                                 실패 시, None을 반환합니다.
        """
        # 클라이언트가 정상적으로 초기화되었는지 확인합니다.
        if not self.client:
            print("❌ OpenAI 클라이언트가 초기화되지 않아 임베딩을 생성할 수 없습니다.")
            return None
        
        # 입력된 텍스트가 유효한지(비어있지 않고, 문자열 타입인지) 확인합니다.
        if not text or not isinstance(text, str):
            print("⚠️ 임베딩할 텍스트가 유효하지 않습니다. (빈 문자열 또는 None)")
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
            print(f"❌ 텍스트 임베딩 생성 중 오류가 발생했습니다: {e}")
            return None
