# 🗂 Project Structure

```text
SKN22-4th-4Team/
├── config/                     # Django 프로젝트 전역 설정 (settings, urls, wsgi)
├── finance_app/                # 메인 Django 애플리케이션
│   ├── models.py               # DB 모델 (Watchlist, Notification)
│   ├── views.py                # Views (Home, Chat SSE, Calendar, Watchlist/Notification API)
│   ├── report_views.py         # Views (Report 생성 및 다운로드 API 분리)
│   ├── urls.py                 # URL 라우팅
│   ├── templates/              # HTML 템플릿
│   │   ├── base.html           # 레이아웃 뼈대 (Theme Switcher 적용)
│   │   ├── includes/           # navbar (알림 벨), sidebar (관심기업) 공용 컴포넌트
│   │   └── finance_app/        # 페이지 뷰 (home, chat, report, calendar, signup)
│   └── migrations/             # Django DB 마이그레이션
├── static/                     # CSS/JS/Images 정적 자산
│   └── css/
│       └── premium_dark.css    # Glassmorphism 다크/라이트 커스텀 테마
├── src/                        # 핵심 데이터 파이프라인 및 비즈니스 로직
│   ├── core/                   # 코어 로직 (Validator, ChatConnector + 스트리밍)
│   ├── data/                   # API 클라이언트 (Finnhub, Supabase, SEC)
│   ├── prompts/                # 시스템/챗봇 프롬프트 관리
│   ├── rag/                    # RAG 엔진
│   │   ├── analyst_chat.py     # 챗봇 비즈니스 로직 (chat + chat_stream SSE)
│   │   ├── rag_base.py         # RAG 기반 클래스 (LLM 스트리밍 헬퍼)
│   │   ├── chat_tools.py       # 채팅 도구 정의 (ToolExecutor)
│   │   ├── graph_rag.py        # Neo4j + NetworkX 그래프 분석
│   │   ├── llm_client.py       # 통합 LLM Client (Gemini/OpenAI + 스트리밍)
│   │   ├── report_generator.py # 투자 리포트 생성기
│   │   └── vector_store.py     # 벡터 검색 (Supabase pgvector)
│   ├── services/               # 핵심 비즈니스 로직 및 외부 연동 서비스
│   │   └── news_analyzer.py    # Finnhub + FinBERT + GraphRAG 지능형 뉴스 분석
│   ├── tools/                  # 환율, 즐겨찾기, 웹 검색 (Tavily) 통합 도구
│   └── utils/                  # 공통 유틸리티
│       ├── plotly_charts.py    # Plotly 차트 생성 (Line, Candle, Volume, Financial)
│       ├── pdf_utils.py        # PDF 컴파일 유틸리티
│       ├── llm_parser.py       # LLM 응답 파서 (4단계 폴백)
│       └── ticker_resolver.py  # 티커 변환/검증 (한글→티커, 유효성 검사)
├── scripts/                    # 유틸리티 및 배치 스크립트
│   ├── build_company_relationships.py  # [ETL] 기업 관계 추출 및 구축
│   ├── migrate_to_neo4j.py            # Supabase → Neo4j 관계 데이터 마이그레이션
│   ├── sp500_scheduler.py             # FinBERT 뉴스 분석 자동 스케줄러
│   └── upload_to_supabase.py          # 초기 데이터 업로드
├── docs/                       # 프로젝트 문서
│   ├── SECURITY_SYSTEM.md      # 보안 시스템 가이드
│   ├── TUTORIAL.md             # 사용 튜토리얼
│   └── images/                 # 스크린샷 및 이미지
├── 01_data_preprocessing/      # 데이터 전처리 과정 문서
├── 02_system_architecture/     # 시스템 아키텍처 문서
├── models/                     # ML 모델 파일 (gitignored)
├── fonts/                      # 폰트 파일 (gitignored)
├── manage.py                   # Django 관리 도구
├── requirements.txt            # 파이썬 의존성
├── .env                        # 환경 변수 설정 (gitignored)
└── STRUCTURE.md                # 구조 문서 (현재 파일)
```
