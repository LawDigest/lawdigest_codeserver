<div align="center">

# LawDigest Data Pipeline

**Data collection, processing, and loading pipelines for LawDigest**

![Python 3.10+](https://img.shields.io/badge/Python_3.10+-3776AB?style=flat-square&logo=python&logoColor=white) ![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white) ![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white) ![Qdrant](https://img.shields.io/badge/Qdrant-DC244C?style=flat-square) ![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white)

[한국어](./README.md)

</div>

---

## Overview

This repository collects, processes, and loads bill, lawmaker, timeline, result, vote, and alternative-bill relationship data for LawDigest.

The current orchestration standard is Airflow. n8n assets and some manual scripts are retained for legacy/debugging purposes and should not be scheduled in parallel with Airflow.

## Highlights

| Area | Description |
|---|---|
| Airflow operation | Runs around `lawdigest_hourly_update_dag`, `manual_collect_bills`, and `lawdigest_daily_db_backup_dag`. |
| Collection tools | `tools/collect_*.py` scripts collect lawmakers, bills, timelines, results, and votes. |
| Direct DB loading | Legacy n8n/direct DB paths are retained for experiments and debugging. |
| AI/RAG extensions | Includes OpenAI, Gemini, Qdrant, LangChain, and llama-index dependencies. |
| Test assets | Includes pytest coverage for DataFetcher, DatabaseManager, and AI summarizer flows. |

## Repository Structure

| Path | Role |
|---|---|
| src/lawdigest_data_pipeline/ | Core collection, processing, persistence, reporting, and summarization modules |
| src/lawdigest_ai/ | Embedding and Qdrant helpers |
| airflow/dags/ | Airflow DAG definitions |
| tools/ | Manual collection scripts and usage guide |
| tests/ | pytest regression and integration tests |

## Quick Start

### Create virtualenv

```bash
python -m venv .venv && source .venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Install test dependencies

```bash
pip install -r requirements_test.txt
```

### Start Airflow

```bash
./scripts/airflow_control.sh up
```

### Unpause main DAGs

```bash
./scripts/airflow_control.sh unpause-main
```

## Verification

| Check | Command |
|---|---|
| Run tests | `python -m pytest tests` |
| List DAGs | `./scripts/airflow_control.sh list-dags` |
| Manual hourly trigger | `./scripts/airflow_control.sh trigger-hourly 2026-03-01 2026-03-08 22` |

## Operational Notes

- Airflow is the single operational standard.
- n8n scripts are retained as legacy references.
- DB/API/secret values should live in `.env` or runtime secrets, not in Git.

## Documentation Sources

This README was written from the following files and documents in this repository.

- `README.md`
- `pyproject.toml`
- `tools/USAGE.md`
- `AGENTS.md`
- `scripts/airflow_control.sh`
