# Tools 디렉토리 스크립트 사용법

이 문서에는 `tools` 디렉토리에 포함된 각 Python 스크립트의 사용법이 정리되어 있습니다.

## 목차
1.  [`collect_alternatives.py`](#collect_alternativespy)
2.  [`collect_bills.py`](#collect_billspy)
3.  [`collect_lawmakers.py`](#collect_lawmakerspy)
4.  [`collect_results.py`](#collect_resultspy)
5.  [`collect_timeline.py`](#collect_timelinepy)
6.  [`collect_votes.py`](#collect_votespy)

---

### `collect_alternatives.py`

특정 국회 대수 범위의 대안-법안 관계 데이터를 수집합니다.

**사용법:**
```bash
python tools/collect_alternatives.py --start-ord <시작_국회_대수> --end-ord <종료_국회_대수>
```

**인자:**
-   `--start-ord`: 데이터 수집을 시작할 국회 대수 (예: `21`)
-   `--end-ord`: 데이터 수집을 종료할 국회 대수 (예: `21`)

**예시:**
```bash
python tools/collect_alternatives.py --start-ord 21 --end-ord 21
```

---

### `collect_bills.py`

지정된 기간과 국회 대수에 해당하는 법안 데이터를 수집합니다.

**사용법:**
```bash
python tools/collect_bills.py --start-date <시작_날짜> --end-date <종료_날짜> --age <국회_대수>
```

**인자:**
-   `--start-date`: 데이터 수집 시작 날짜 (형식: `YYYY-MM-DD`)
-   `--end-date`: 데이터 수집 종료 날짜 (형식: `YYYY-MM-DD`)
-   `--age`: 국회 대수 (예: `21`)

**예시:**
```bash
python tools/collect_bills.py --start-date 2024-01-01 --end-date 2024-01-31 --age 21
```

---

### `collect_lawmakers.py`

전체 국회의원 데이터를 수집합니다. 이 스크립트는 별도의 인자를 받지 않습니다.

**사용법:**
```bash
python tools/collect_lawmakers.py
```

---

### `collect_results.py`

지정된 기간과 국회 대수에 해당하는 법안 처리 결과 데이터를 수집합니다.

**사용법:**
```bash
python tools/collect_results.py --start-date <시작_날짜> --end-date <종료_날짜> --age <국회_대수>
```

**인자:**
-   `--start-date`: 데이터 수집 시작 날짜 (형식: `YYYY-MM-DD`)
-   `--end-date`: 데이터 수집 종료 날짜 (형식: `YYYY-MM-DD`)
-   `--age`: 국회 대수 (예: `21`)

**예시:**
```bash
python tools/collect_results.py --start-date 2024-01-01 --end-date 2024-01-31 --age 21
```

---

### `collect_timeline.py`

지정된 기간과 국회 대수에 해당하는 법안 타임라인 데이터를 수집합니다.

**사용법:**
```bash
python tools/collect_timeline.py --start-date <시작_날짜> --end-date <종료_날짜> --age <국회_대수>
```

**인자:**
-   `--start-date`: 데이터 수집 시작 날짜 (형식: `YYYY-MM-DD`)
-   `--end-date`: 데이터 수집 종료 날짜 (형식: `YYYY-MM-DD`)
-   `--age`: 국회 대수 (예: `21`)

**예시:**
```bash
python tools/collect_timeline.py --start-date 2024-01-01 --end-date 2024-01-31 --age 21
```

---

### `collect_votes.py`

지정된 기간과 국회 대수에 해당하는 법안 표결 정보를 수집합니다.

**사용법:**
```bash
python tools/collect_votes.py --start-date <시작_날짜> --end-date <종료_날짜> --age <국회_대수>
```

**인자:**
-   `--start-date`: 데이터 수집 시작 날짜 (형식: `YYYY-MM-DD`)
-   `--end-date`: 데이터 수집 종료 날짜 (형식: `YYYY-MM-DD`)
-   `--age`: 국회 대수 (예: `21`)

**예시:**
```bash
python tools/collect_votes.py --start-date 2024-01-01 --end-date 2024-01-31 --age 21
```
