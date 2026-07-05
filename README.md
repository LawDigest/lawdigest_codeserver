<div align="center">

# LawDigest Data Pipeline

**모두의입법을 위한 국회 데이터 수집·가공·적재 파이프라인**

![Python 3.10+](https://img.shields.io/badge/Python_3.10+-3776AB?style=flat-square&logo=python&logoColor=white) ![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white) ![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white) ![Qdrant](https://img.shields.io/badge/Qdrant-DC244C?style=flat-square) ![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white)

[English](./README.en.md)

</div>

---

## 소개

이 저장소는 모두의입법 서비스에 필요한 법안, 의원, 타임라인, 처리 결과, 표결, 대안 관계 데이터를 수집하고 정제해 저장하는 데이터 파이프라인입니다.

운영 기준은 Airflow DAG입니다. n8n 관련 자산과 일부 수동 스크립트는 레거시 또는 디버깅 용도로 남아 있으며, Airflow와 동시에 스케줄링하지 않는 것이 원칙입니다.

## 주요 기능

| 기능 | 설명 |
|---|---|
| Airflow 운영 | `lawdigest_hourly_update_dag`, `manual_collect_bills`, `lawdigest_daily_db_backup_dag`를 기준으로 운영합니다. |
| 수집 도구 | `tools/collect_*.py` 스크립트로 의원, 법안, 타임라인, 결과, 표결 데이터를 수집합니다. |
| 직접 DB 적재 | 레거시 n8n DB pipeline과 직접 DB 모드를 실험/디버깅용으로 보관합니다. |
| AI/RAG 확장 | OpenAI, Gemini, Qdrant, LangChain/llama-index 의존성을 포함합니다. |
| 테스트 자산 | pytest 기반 DataFetcher, DatabaseManager, AI summarizer 테스트가 있습니다. |

## 저장소 구조

| 경로 | 역할 |
|---|---|
| src/lawdigest_data_pipeline/ | Core collection, processing, persistence, reporting, and summarization modules |
| src/lawdigest_ai/ | Embedding and Qdrant helpers |
| airflow/dags/ | Airflow DAG definitions |
| tools/ | Manual collection scripts and usage guide |
| tests/ | pytest regression and integration tests |

## 빠른 시작

### 가상환경 생성

```bash
python -m venv .venv && source .venv/bin/activate
```

### 의존성 설치

```bash
pip install -r requirements.txt
```

### 테스트 의존성 설치

```bash
pip install -r requirements_test.txt
```

### Airflow 시작

```bash
./scripts/airflow_control.sh up
```

### 주요 DAG 활성화

```bash
./scripts/airflow_control.sh unpause-main
```

## 검증

| 항목 | 명령 |
|---|---|
| Run tests | `python -m pytest tests` |
| List DAGs | `./scripts/airflow_control.sh list-dags` |
| Manual hourly trigger | `./scripts/airflow_control.sh trigger-hourly 2026-03-01 2026-03-08 22` |

## 운영 메모

- 운영 기준은 Airflow 단일 운영입니다.
- n8n 관련 스크립트는 레거시 참고용입니다.
- DB/API/secret 값은 `.env`나 운영 secret으로 관리하고 Git에 넣지 않습니다.

## 문서 작성 근거

이 README는 저장소 안의 다음 파일과 문서를 기준으로 작성했습니다.

- `README.md`
- `pyproject.toml`
- `tools/USAGE.md`
- `AGENTS.md`
- `scripts/airflow_control.sh`
