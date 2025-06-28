import pandas as pd
from IPython.display import clear_output
from langchain.chat_models import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
import os
from dotenv import load_dotenv

class AISummarizer:

    def __init__(self):
        self.input_data = None
        self.output_data = None
        self.proposer_type_list = ['congressman', 'chairman', 'gov'] # TODO: 자동 발의자 구분 인식 구현 이후 제거
        self.prompt_dict = {
            '의원':  "너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"{proposer}이 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1.핵심 내용: 설명 2.핵심 내용: 설명 3.핵심 내용: 설명 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해. 핵심 내용은 볼드체 처리해.",
            '위원장': "너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"{proposer}이 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1.핵심 내용: 설명 2.핵심 내용: 설명 3.핵심 내용: 설명 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해. 핵심 내용은 볼드체 처리해.",
            '정부':  "너는 법률개정안을 이해하기 쉽게 요약해서 알려줘야 해. 반드시 \"대한민국 {proposer}가 발의한 {title}의 내용 및 목적은 다음과 같습니다:\"로 문장을 시작해. 1.핵심 내용: 설명 2.핵심 내용: 설명 3.핵심 내용: 설명 이렇게 쉽게 요약하고, 마지막은 법안의 취지를 설명해. 핵심 내용은 볼드체 처리해."
        }

        # ChatGPT model via Langchain
        self.api_key = os.environ.get("APIKEY_OPENAI")

        # 환경변수 로드
        load_dotenv()

    def AI_title_summarize(self, df_bills, model=None):
    
        if model is None:
            model = os.environ.get("TITLE_SUMMARIZATION_MODEL")

        llm = ChatOpenAI(model=model, openai_api_key=self.api_key)
        
        print("\n[AI 제목 요약 진행 중...]")
        
        # 'briefSummary' 컬럼이 공백이 아닌 경우에만 요약문을 추출하여 해당 컬럼에 저장
        total = df_bills['briefSummary'].isnull().sum()
        count = 0
        show_count = 0

        for index, row in df_bills.iterrows():
            count += 1
            print(f"현재 진행률: {count}/{total} | {round(count/total*100, 2)}%")

            content, title, id, proposer = row['summary'], row['billName'], row['billNumber'], row['proposers']
            print('-'*10)
            if not pd.isna(row['briefSummary']):
                print(f"{title}에 대한 요약문이 이미 존재합니다.")
                # clear_output()
                continue  
                # 이미 'SUMMARY', 'GPT_SUMMARY' 컬럼에 내용이 있으면 건너뜁니다

            task = f"\n위 내용의 핵심을 40글자 이내로 짧게 요약한 제목을 작성할 것. 제목은 반드시 {title}으로 끝나야 함."
            print(f"task: {task}")
            print('-'*10)

            messages = [
                SystemMessage(content="입력하는 법률개정안 내용의 핵심을 40글자 이내로 짧게 요약한 제목을 한 문장으로 작성할 것. 제목은 반드시 법률개정안 이름으로 끝나야 함.\n\n법률개정안의 내용을 한눈에 알아볼 수 있게 핵심을 요약한 제목을 작성. 반드시 '~하기 위한 ~법안'와 같은 형식으로 작성. 반드시 한 문장으로 작성. 법안의 취지를 중심으로 짧고 간결하게 요약\n"),
                HumanMessage(content=str(content) + str(task))
            ]

            chat_response = llm.invoke(messages).content

            print(f"chatGPT: {chat_response}")

            # 추출된 요약문을 'briefSummary' 컬럼에 저장
            df_bills.loc[df_bills['billNumber'] == id, 'briefSummary'] = chat_response
            show_count += 1

            if show_count % 5 == 0:
                clear_output()
        
        print(f"[법안 {count}건 요약 완료됨]")

        clear_output()
        
        print("[AI 제목 요약 완료]")

        self.output_data = df_bills

        return df_bills

    def AI_content_summarize(self, df_bills, model=None):
        """
        df_bills를 입력받아 'proposerKind' 컬럼을 기준으로 발의주체별 프롬프트를 자동으로 적용하여 AI 요약을 생성합니다.
        """
        if model is None:
            model = os.environ.get("CONTENT_SUMMARIZATION_MODEL")

        llm = ChatOpenAI(model=model, openai_api_key=self.api_key)

        print("\n[AI 내용 요약 진행 중...]")

        rows_to_process = df_bills[df_bills['gptSummary'].isnull()]
        total = len(rows_to_process)
        
        if total == 0:
            print("[모든 법안에 대한 AI 요약이 이미 존재합니다.]")
            self.output_data = df_bills
            return df_bills

        count = 0
        
        for index, row in rows_to_process.iterrows():
            count += 1
            print(f"현재 진행률: {count}/{total} | {round(count/total*100, 2)}%")

            content, title, bill_id, proposer = row['summary'], row['billName'], row['billNumber'], row['proposers']
            proposer_kind = row['proposerKind'] # '의원', '위원장', '정부'
            
            print('-'*10)
            
            # 1. 'proposerKind' 값을 키로 사용해 prompt_dict에서 직접 템플릿 가져오기
            #    .get()을 사용하여 해당 키가 없는 경우에도 오류 없이 안전하게 처리합니다.
            prompt_template = self.prompt_dict.get(proposer_kind)
            
            if not prompt_template:
                print(f"경고: '{proposer_kind}'에 해당하는 프롬프트 템플릿이 없습니다. (법안: {title})")
                continue

            # 2. 선택된 프롬프트 템플릿 포맷팅
            system_prompt = prompt_template.format(proposer=proposer, title=title)
            
            task = f"\n위 내용은 {title}이야. 이 법률개정안에서 무엇이 달라졌는지 제안이유 및 주요내용을 쉽게 요약해줘."
            print(f"task: {task}")
            print('-'*10)
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=str(content) + str(task))
            ]

            try:
                chat_response = llm.invoke(messages).content
                print(f"chatGPT: {chat_response}")
                
                df_bills.loc[index, 'gptSummary'] = chat_response
            except Exception as e:
                print(f"[API 호출 오류] 법안: {title}, 오류: {e}")
                continue

            if count % 5 == 0 and count < total:
                # clear_output(wait=True)
                pass
        
        # clear_output()
        print(f"\n[법안 {count}건 요약 완료됨]")
        print("[AI 내용 요약 완료]")

        self.output_data = df_bills
        return df_bills

    def AI_model_test(date=None, title_model=None, content_model=None):
        pass


# TODO: APISender 구현
