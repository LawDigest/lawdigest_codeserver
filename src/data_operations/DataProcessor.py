import pandas as pd
import re

class DataProcessor:


    def __init__(self, fetcher):
        self.fetcher = fetcher
    
    def process_congressman_bills(self, df_bills):
        """의원 발의 법안을 처리하는 함수
        
        Args:
            df_bills (pd.DataFrame) : 수집한 법안 데이터
        Return:
            df_bills_congressman (pd.DataFrame) : 처리된 의원 발의 법안 데이터
        """
        df_bills_congressman = df_bills[df_bills['proposerKind'] == '의원'].copy()

        print(f"[의원 발의 법안 개수: {len(df_bills_congressman)}]")
        
        if len(df_bills_congressman) == 0:
            print("[의원 발의 법안이 없습니다. 코드를 종료합니다.]")
            return pd.DataFrame()
        
        # --- TODO: 의원 법안 데이터용 proposers 컬럼 생성 ---
        # billName 컬럼 값에서 정규표현식을 사용하여 괄호 안의 발의자 정보를 추출합니다.
        # 예: '조세특례제한법 일부개정법률안(김형동의원 등 11인)' -> '김형동의원 등 11인'
        print("\n[billName에서 발의자 정보 추출 중...]")
        df_bills_congressman['proposers'] = df_bills_congressman['billName'].str.extract(r'\(([^)]+)\)$').fillna('')
        print("[발의자 정보 추출 완료]")
        
        # billName 컬럼에서 추출한 발의자 정보(괄호 포함)를 제거합니다.
        print("\n[billName에서 발의자 정보 제거 중...]")
        df_bills_congressman['billName'] = df_bills_congressman['billName'].str.replace(r'\s*\([^)]+\)$', '', regex=True).str.strip()
        print("[billName 정리 완료]")

        # df_bills_congressman에 발의자 정보 컬럼 머지
        print("\n[의원 발의자 데이터 병합 중...]")
        df_coactors = self.fetcher.fetch_bills_coactors()
        df_bills_congressman = pd.merge(df_bills_congressman, df_coactors, on='billId', how='inner')
        print("[의원 발의자 데이터 병합 완료]")

        # 아래 로직은 'proposers' 컬럼이 생성된 후에 실행됩니다.
        def extract_names(proposer_str):
            # '의원'이라는 단어 앞에 있는 한글 이름을 모두 찾습니다.
            return re.findall(r'[가-힣]+(?=의원)', proposer_str) if isinstance(proposer_str, str) else []

        df_bills_congressman['rstProposerNameList'] = df_bills_congressman['proposers'].apply(extract_names)


        if 'publicProposerIdList' not in df_bills_congressman.columns:
            df_bills_congressman['publicProposerIdList'] = [
                [f'ID_{i}{j}' for j in range(1, 20)] for i in range(len(df_bills_congressman))
            ]

        def get_proposer_codes(row):
            name_list_length = len(row['rstProposerNameList'])
            # 이름 목록의 길이에 맞춰 ID 리스트를 자릅니다.
            return row['publicProposerIdList'][:name_list_length]

        # 새로운 컬럼 rstProposerIdList에 publicProposerIdList 리스트에서 슬라이싱한 값 추가
        df_bills_congressman['rstProposerIdList'] = df_bills_congressman.apply(get_proposer_codes, axis=1)

        print(f"\n[처리 후 의원 발의 법안 개수: {len(df_bills_congressman)}]")

        # 제외할 컬럼 목록
        # ProposerName 컬럼이 존재할 경우에만 삭제 시도
        columns_to_drop = ['rstProposerNameList']
        if 'ProposerName' in df_bills_congressman.columns:
            columns_to_drop.append('ProposerName')
            
        df_bills_congressman.drop(columns=columns_to_drop, inplace=True, errors='ignore')

        return df_bills_congressman

    def process_chairman_bills(self, df_bills):
        """ 위원장 발의 법안을 처리하는 함수
        
        Args:
            df_bills (pd.DataFrame) : 수집한 법안 데이터
            fetcher (DataFetcher) : 데이터 수집용 객체 - 대안-법안 관계 수집 위해 필요

        Returns:
            df_bills_chairman (pd.DataFrame) : 처리된 위원장 발의 법안 데이터
            df_alternatives (pd.DataFrame) : 위원장 발의 법안 데이터에 대한 대안-법안 포함관계 데이터
        """

        df_bills_chair = df_bills[df_bills['proposerKind'] == '위원장'].copy()

        if(len(df_bills_chair) == 0):
            print("[위원장 발의 법안이 없습니다.]")
            return pd.DataFrame(), pd.DataFrame()

        # 위원장안 - 포함된 의원 관계 데이터 수집
        df_alternatives = self.fetcher.fetch_bills_alternatives(df_bills)

        # df_bills_chair의 billName에서 (대안) 제거
        df_bills_chair['billName'] = df_bills_chair['billName'].str.replace(r'\(대안\)', '', regex=True)

        # TODO: 의장 법안 데이터용 proposers 컬럼 추출
        # 위 의원 데이터 처리에서와 같은 로직 적용 가능

        return df_bills_chair, df_alternatives

    def process_gov_bills(self, df_bills):
        """ 정부 발의 법안을 처리하는 함수

        Args: 
            df_bills (pd.DataFrame) : 수집한 법안 데이터
        
        Returns:
            df_bills_gov (pd.DataFrame) : 처리된 정부 발의 법안 데이터
        """
    
        df_bills_gov = df_bills[df_bills['proposerKind'] == '정부'].copy()

        if(len(df_bills_gov) == 0):
            print("[정부 발의 법안이 없습니다.]")
            return pd.DataFrame()

        # TODO: 정부 법안 데이터용 proposers 컬럼 추출

        return df_bills_gov

    def merge_bills_df(self, df_bills_content, df_bills_info):
        print("\n[데이터프레임 병합 진행 중...]")
        # 'billNumber' 컬럼을 기준으로 두 데이터프레임을 병합
        df_bills = pd.merge(df_bills_content, df_bills_info, on='billNumber', how='inner')

        # BILL_NO가 중복되는 행 제거
        # df_bills = df_bills.drop_duplicates(subset='BILL_NO', keep='first')
        
        #billNumber가 중복되는 행 중 proposers가 '대통령'인 행 제거 => 대통령 거부권 행사한 법안 중복 제거
        df_bills = df_bills[~((df_bills['proposers'] == '대통령') & (df_bills['billNumber'].duplicated()))]

        # BILL_ID 결측치가 있는 행 제거
        df_bills = df_bills.dropna(subset=['billId'])

        #인덱스 재설정
        df_bills.reset_index(drop=True, inplace=True)
        
        print("데이터프레임 병합 완료")
        print(f"{len(df_bills)} 개의 법안 데이터 병합됨.")
        
        print(df_bills['proposeDate'].value_counts())
        
        return df_bills

    def add_AI_summary_columns(self, df_bills):
        df_bills['briefSummary'] = None
        df_bills['gptSummary'] = None
        print("\n[AI 요약 데이터 컬럼 추가 완료]")

        return df_bills

    def remove_duplicates(self, df_bills, DBManager):
        print("\n[DB와의 중복 데이터 제거 중...]")

        # 법안 ID 리스트 추출
        bill_ids = df_bills['billId'].tolist()

        # 기존 법안 ID 조회
        existing_ids = DBManager.get_existing_bill_ids(bill_ids)

        # 데이터프레임에서 DB에 존재하지 않는 법안 ID만 남기기
        df_bills_dup_removed = df_bills[~df_bills['billId'].isin(existing_ids)]
        
        # 중복 처리 결과 출력
        print(f"[총 {len(df_bills)}개의 법안 데이터 중 {len(df_bills_dup_removed)}개의 새로운 법안 데이터 발견됨.]")

        print(df_bills_dup_removed['proposeDate'].value_counts())

        return df_bills_dup_removed
