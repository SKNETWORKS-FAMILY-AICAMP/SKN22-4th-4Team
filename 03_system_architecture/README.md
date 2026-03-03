# 02. 시스템 아키텍처 (System Architecture)

## 📌 개요

본 프로젝트는 **Hybrid RAG (Vector Search + Graph Analysis)** 기반의 금융 분석 챗봇 시스템입니다.

## 🏗️ 전체 아키텍처

```mermaid
graph TD
    User(["👤 사용자"]) -->|1. 접속| Login["🔐 로그인/회원가입"]
    Login -->|2. 인증 성공| UI["💻 Django Web App (Premium UI)"]
    
    subgraph Frontend Logic
        Login -->|Auth Request| Auth["🔐 Django Auth"]
        UI -->|Chat Query| Validator["🛡️ Input Validator"]
        UI -->|Report Request| ReportTask["⚡ Celery Task Queue"]
        UI -->|Manage Favorites| Watchlistmgr["⭐ Watchlist Manager"]
    end

    subgraph "Background Processing (Celery + Redis)"
        ReportTask -->|Broker| Redis[("🟥 Redis Broker & Result Backend")]
        Redis -->|Consume Task| CeleryWorker["⚙️ Celery Worker (heavy/default)"]
        CeleryWorker -->|Generate| ReportGen["📝 Report Generator"]
        CeleryWorker -->|Notify| UI
    end

    subgraph "Data & State"
        Auth <-->|Verify| UserDB[("👥 SQLite DB")]
        Watchlistmgr <-->|Sync| UserDB
    end

    subgraph RAG Engine
        Validator -->|Valid Query| Agent["🤖 LLM Client"]
        ReportGen -->|Data Fetch| Retriever["🔍 Data Retriever"]
        
        Agent <-->|Vector Search| VectorDB[("🗄️ Supabase pgvector")]
        Agent <-->|Graph Search| GraphDB[("🕸️ Neo4j GraphDB")]
        
        Retriever -->|Parallel Fetch| VectorDB
        Retriever -->|Parallel Fetch| GraphDB
    end

    subgraph Data Sources
        Retriever -->|Live Price/News| Finnhub["📡 Finnhub API"]
        Retriever -->|Market Info| Yahoo["📈 yfinance API"]
        Retriever -->|Unknown Ticker| Tavily["🕵️ Tavily Search"]
        VectorDB <-->|Sync| SEC["📄 SEC 10-K"]
    end

    Retriever -->|Aggregated Context| LLM["🧠 Gemini 2.5 Flash (Primary) / GPT Fallback"]
    Agent -->|Final Answer| LLM
    LLM -->|Response| UI
    LLM -.->|Tracing| LS["📊 LangSmith"]
```

### 1. Frontend (User Interface)

- **Framework**: Django (MVT Pattern) + Bootstrap (Premium Glassmorphism)
- **Features**:
  - 실시간 채팅 인터페이스 (`AnalystChatbot`)
  - 대화형 차트 및 데이터 시각화
  - 사용자 및 관심 기업 관리 (Sidebar)

### 2. Backend & AI Engine

- **Background Processing**:
  - **Celery & Redis**: 시간이 오래 걸리는 투자 리포트 생성 및 뉴스 감성 분석 작업을 백그라운드 큐(heavy, default)로 분리 처리하여 타임아웃 방지 및 응답성 극대화
- **RAG Engine**:
  - **Vector Store**: 텍스트 의미 검색 (Semantic Search)
  - **GraphRAG**: **Neo4j** Cypher 쿼리 + `NetworkX` 기반 기업 관계망 분석
  - **Hybrid Retrieval**: 벡터 검색 결과와 그래프 분석 결과를 결합하여 답변 생성
- **FinBERT NLP Pipeline** (`src/services/news_analyzer.py`):
  - **감성 분석 정밀도**: Financial PhraseBank 데이터셋(Full Agreement 기준)에서 **약 97% 정확도** 및 98% 수준의 F1-Score를 달성한 검증된 모델 사용
  - **GraphRAG 확장**: 단일 기업 타겟을 넘어 대상 기업의 공급망 및 파트너사의 뉴스까지 긁어와 다차원 리스크 모니터링
  - 매일 06:00 (KST) 스케줄러가 파이프라인을 자동 구동하여 Supabase에 적재
- **LLM**: Gemini 2.5 Flash 기본 / GPT-4o-mini 선택적 (`.env`로 전환)
- **LLM Client**: `llm_client.py` — Gemini/OpenAI 통합 추상화 레이어
- **LangSmith**: LLM 콜 트레이싱 및 모니터링 (선택적)

### 3. Database & Infrastructure

- **Supabase (PostgreSQL)**:
  - `pgvector`: 벡터 임베딩 저장 및 검색
  - `Relational Tables`: 기업 정보, 사용자 정보, 관계 데이터 관리
- **Neo4j**: 기업 관계망 그래프 DB (614노드, 212관계)
- **Authentication**: Supabase Auth

## 🔄 데이터 흐름 (Data Flow)

1. **User Query**: 사용자가 질문 입력 (예: "애플의 공급망 리스크는?")
2. **Intent Analysis**: 질문 의도 파악 (일반 대화 vs 분석 요청)
3. **Information Retrieval**:
   - **Vector Search**: 관련 뉴스/보고서 검색 (`documents`)
   - **Graph Search**: 관련 기업 네트워크 및 관계 탐색 (`company_relationships`)
4. **Context Assembly**: 검색된 텍스트와 그래프 정보를 프롬프트로 구성
5. **Generation**: LLM이 분석 결과 생성 및 답변 제공

---

## 🕸️ GraphRAG: 지능형 관계망 분석

단순한 텍스트 검색(Vector RAG)을 넘어, 기업 간의 **공급망(Supply Chain), 경쟁 구도, 지배 구조**를 연결하여 입체적인 분석을 제공합니다.

### GraphRAG 작동 원리 (Architecture)

```mermaid
graph TD
    subgraph "1. 데이터 추출 (Ingestion)"
        A[Original Text / 10-K] --> B{LLM 관계 추출}
        B -- "추출" --> C(JSON: Source-Target-Relationship)
        C -- "저장" --> D[(Neo4j + Supabase)]
    end

    subgraph "2. 네트워크 구축 (Graph Building)"
        D --> E[Neo4j Cypher 쿼리 / NetworkX 분석]
        E --> F[Nodes: 기업/브랜드]
        E --> G[Edges: 파트너, 경쟁사, 자회사 등]
    end

    subgraph "3. 지능형 검색 (Query)"
        H[사용자 질문] --> I{그래프 탐색}
        I --> J[Neo4j 관계 쿼리 + 중심성 분석]
        J --> K[최단 경로 / 네트워크 분석]
        K --> L[그래프 컨텍스트 생성]
    end

    subgraph "4. 인사이트 생성"
        L --> M{Gemini 2.5 Flash}
        M --> N[입체적 투자 인사이트 답변]
    end
    end
```

### 주요 기능

- **관계망 추론**: 특정 기업의 악재가 공급망 내 어떤 기업에 파급될지 분석합니다.
- **네트워크 위치 분석**: NetworkX의 `Centrality(중심성)` 알고리즘을 사용하여 시장 내 핵심 기업을 식별합니다.
- **다차원 컨텍스트**: 벡터 검색 결과와 그래프 분석 결과를 결합하여 정보의 누락 없는 답변을 생성합니다.

---

## 🛠️ 기술 스택 (Tech Stack)

- **Language**: Python 3.12
- **Message Broker & Queue**: Celery, Redis
- **LLM**: Gemini 2.5 Flash (기본) / GPT-4o-mini (선택)
- **Graph DB**: Neo4j (Cypher) + NetworkX (분석)
- **Vector DB**: Supabase pgvector
- **Tracing**: LangSmith (선택적)
- **Key Libraries**: `celery`, `redis`, `google-generativeai`, `openai`, `neo4j`, `supabase`, `networkx`, `django`

## 📂 프로젝트 구조 (Directory Structure)

```bash
SKN22-4th-4Team/
├── config/                     # Django 프로젝트 전역 설정 (settings, urls, wsgi)
├── finance_app/                # 메인 Django 애플리케이션 (views, report_views, templates, static)
├── src/                        # 핵심 데이터 파이프라인 및 비즈니스 로직
│   ├── core/                   # 코어 로직 (Validator, ChatConnector)
│   ├── data/                   # API 클라이언트 (Finnhub, Supabase, SEC)
│   ├── prompts/                # 시스템/챗봇 프롬프트 관리
│   ├── rag/                    # RAG 엔진 (Chat, GraphRAG, Report, LLM Client)
│   ├── services/               # 핵심 비즈니스 로직 및 외부 연동 서비스 (FinBERT 뉴스 분석)
│   ├── tools/                  # 환율, 즐겨찾기, 웹 검색 (Tavily) 통합 도구
│   └── utils/                  # 유틸리티 (Plotly 차트, PDF 생성, LLM 파서, 티커 변환)
├── scripts/                    # 유틸리티 및 배치 스크립트
├── docs/                       # 프로젝트 문서 및 이미지
├── manage.py                   # Django 관리 도구
├── requirements.txt            # 파이썬 의존성
└── .env                        # 환경 변수 설정 (gitignored)
```
