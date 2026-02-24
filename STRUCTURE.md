# 🗂 Project Structure

```text
SKN22-4th-4Team/
├── config/                     # Django 프로젝트 전역 설정 (settings, urls, wsgi)
├── finance_app/                # 메인 Django 애플리케이션
│   ├── models.py               # DB 모델 (Watchlist 등)
│   ├── views.py                # Views (Home, Chat, Report, Calendar, Watchlist API)
│   ├── urls.py                 # URL 라우팅
│   ├── templates/              # HTML 템플릿
│   │   ├── base.html           # 레이아웃 뼈대 (Theme Switcher 적용)
│   │   ├── includes/           # navbar, sidebar 등 공용 컴포넌트
│   │   └── finance_app/        # 페이지 뷰 (home, chat, report, calendar, signup)
│   └── migrations/             # Django DB 마이그레이션
├── static/                     # CSS/JS/Images 정적 자산
│   └── css/
│       └── premium_dark.css    # Glassmorphism 다크/라이트 커스텀 테마
├── src/                        # 핵심 데이터 파이프라인 및 비즈니스 로직
│   ├── core/                   # 코어 로직 (Validator, ChatConnector)
│   ├── data/                   # API 클라이언트 (Finnhub, Supabase, SEC)
│   ├── prompts/                # 시스템/챗봇 프롬프트 관리
│   ├── rag/                    # RAG 엔진
│   │   ├── analyst_chat.py     # 챗봇 비즈니스 로직
│   │   ├── graph_rag.py        # Neo4j + NetworkX 그래프 분석
│   │   ├── llm_client.py       # 통합 LLM Client (Gemini/OpenAI)
│   │   ├── report_generator.py # 투자 리포트 생성기
│   │   └── vector_store.py     # 벡터 검색 (Supabase pgvector)
│   ├── sql/                    # SQL 관련 모듈
│   ├── tools/                  # 환율, 즐겨찾기, 검색 도구
│   └── utils/                  # 유틸리티 (Plotly 차트, PDF, 티커 변환)
├── scripts/                    # 유틸리티 및 배치 스크립트
│   ├── build_company_relationships.py  # [ETL] 기업 관계 추출 및 구축
│   ├── migrate_to_neo4j.py            # Supabase → Neo4j 관계 데이터 마이그레이션
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
