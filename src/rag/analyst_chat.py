"""
Analyst Chatbot - 애널리스트/기자 스타일 챗봇
Uses Gemini 2.5 Flash (or OpenAI fallback) with RAG context
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

import json
import re

try:
    from langsmith import traceable
except ImportError:

    def traceable(*args, **kwargs):
        def decorator(func):
            return func

        if args and callable(args[0]):
            return args[0]
        return decorator


try:
    from rag.rag_base import RAGBase, EXCHANGE_AVAILABLE
except ImportError:
    from src.rag.rag_base import RAGBase, EXCHANGE_AVAILABLE

logger = logging.getLogger(__name__)

# Prompts directory
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


class AnalystChatbot(RAGBase):
    """
    애널리스트/기자 스타일로 금융 정보를 분석하고 답변하는 챗봇
    Gemini 2.5 Flash 사용 (OpenAI fallback)
    """

    def __init__(self):
        """Initialize chatbot inheriting from RAGBase"""
        self.model_name = os.getenv("CHAT_MODEL", "gemini-2.5-flash")
        super().__init__(model_name=self.model_name)

        # Exchange rate client (Special for Chatbot)
        self.exchange_client = None
        if EXCHANGE_AVAILABLE:
            try:
                from tools.exchange_rate_client import get_exchange_client

                self.exchange_client = get_exchange_client()
            except ImportError:
                try:
                    from src.tools.exchange_rate_client import get_exchange_client

                    self.exchange_client = get_exchange_client()
                except Exception as e:
                    logger.warning(f"Exchange client init failed: {e}")

        # Tool executor (분리된 모듈)
        try:
            from rag.chat_tools import ToolExecutor
        except ImportError:
            from src.rag.chat_tools import ToolExecutor

        self.tool_executor = ToolExecutor(
            finnhub=self.finnhub,
            exchange_client=self.exchange_client,
            register_func=self._register_company,
        )

        # Load system prompt with security defense layer
        self.system_prompt = self._load_system_prompt_with_defense()

        # Conversation history
        self.conversation_history: List[Dict] = []
        logger.info("AnalystChatbot initialized (inherited from RAGBase)")

    def _load_system_prompt_with_defense(self) -> str:
        """
        시스템 방어 레이어와 모듈화된 프롬프트 컴포넌트들을 결합하여 로드합니다.
        """
        parts = []

        # 1. 시스템 방어 레이어 로드 (최우선)
        defense_prompt = self._load_prompt("system_defense.txt")
        if defense_prompt:
            parts.append(defense_prompt)
            logger.info("System defense layer loaded")

        # 2. 모듈화된 프롬프트 로드
        # 순서: 역할/원칙 -> 분석/전략 -> 도구 가이드 -> 출력 형식
        components = [
            "components/01_role_principles.txt",
            "components/02_analysis_framework.txt",
            "components/03_tool_guidelines.txt",
            "components/04_output_format.txt",
        ]

        main_prompt_parts = []
        for comp in components:
            content = self._load_prompt(comp)
            if content:
                main_prompt_parts.append(content)
            else:
                logger.warning(f"Prompt component not found: {comp}")

        if main_prompt_parts:
            parts.append("\n\n# === ANALYST INSTRUCTIONS ===\n")
            parts.extend(main_prompt_parts)
            logger.info(f"Loaded {len(main_prompt_parts)} prompt components")
        else:
            logger.error("No prompt components found!")
            parts.append("SYSTEM ERROR: Prompt not loaded.")

        combined = "\n\n".join(parts)
        logger.debug(f"Combined system prompt: {len(combined)} chars")
        return combined

    # _get_embedding Removed - Handled by VectorStore internally
    def _search_documents(self, query: str, limit: int = 5) -> List[Dict]:
        """Search relevant documents"""
        if self.vector_store:
            try:
                return self.vector_store.hybrid_search(query, k=limit)
            except Exception as e:
                logger.error(f"VectorStore search failed: {e}")
        return []

    def _get_company_info(self, ticker: str) -> Optional[Dict]:
        """Get company information"""
        if self.graph_rag:
            try:
                return self.graph_rag.get_company(ticker.upper())
            except Exception as e:
                logger.error(f"GraphRAG get_company failed: {e}")
        return None

    def _get_relationships(self, ticker: str) -> List[Dict]:
        """Get company relationships"""
        if self.graph_rag:
            try:
                data = self.graph_rag.find_relationships(ticker.upper())
                if data:
                    return data.get("outgoing", []) + data.get("incoming", [])
            except Exception as e:
                logger.error(f"GraphRAG find_relationships failed: {e}")
        return []

    def _generate_english_search_query(self, user_query: str) -> str:
        """Translate Korean query to English optimized search query using LLM"""
        try:
            messages = [
                {
                    "role": "system",
                    "content": "You are a search expert. Translate the user's Korean financial question into a precise English search query for finding relevant information in 10-K/10-Q reports. Output ONLY the English query.",
                },
                {"role": "user", "content": user_query},
            ]
            eng_query = self._llm_chat(messages, temperature=0, max_tokens=100)
            logger.info(f"🇺🇸 Translated Query: '{user_query}' -> '{eng_query}'")
            return eng_query
        except Exception as e:
            logger.warning(f"Query translation failed: {e}")
            return user_query  # Fallback to original

    @traceable(run_type="chain", name="build_context")
    def _build_context(self, query: str, ticker: Optional[str] = None) -> str:
        """Build context from RAG search, company data, and real-time Finnhub data (Optimized with Parallel Fetch)"""

        # 0. Translate Query for Better Retrieval (Korean -> English)
        search_query = self._generate_english_search_query(query)

        if not ticker:
            # Ticker가 없는 경우 문서 검색만 수행
            docs = self._search_documents(search_query, limit=5)
            if not docs:
                return "추가 컨텍스트 없음"

            parts = ["## 관련 문서"]
            for doc in docs:
                parts.append(f"- {doc.get('content', '')[:500]}")
            return "\n".join(parts)

        # Ticker가 있는 경우 DataRetriever를 통해 모든 데이터를 병렬로 수집
        if not self.data_retriever:
            return "데이터 수집 모듈 미작동"

        logger.info(
            f"Building context for query: {query} (Search: {search_query}), ticker: {ticker}"
        )
        dataset_context = self.data_retriever.get_company_context_parallel(
            ticker, include_finnhub=True, include_rag=True, query=search_query
        )
        all_data = dataset_context

        context_parts = []

        # 1. Company Info
        company = all_data.get("company")
        if company:
            context_parts.append(f"## 회사 정보: {company.get('company_name', ticker)}")
            context_parts.append(
                f"- 섹터: {company.get('sector', 'N/A')}, 산업: {company.get('industry', 'N/A')}"
            )
            context_parts.append(f"- 시가총액: {company.get('market_cap', 'N/A')}")

        # 2. Relationships (GraphRAG)
        rels = all_data.get("relationships", [])
        if rels:
            context_parts.append(
                f"\n---\n## 🕸️ 기업 관계망 및 공급망 ({len(rels)}개 연결)"
            )
            for rel in rels[:10]:  # Show more relationships (up to 10)
                source = rel.get("source_company")
                target = rel.get("target_company")
                rtype = rel.get("relationship_type", "관련")
                desc = rel.get("description", "")

                # 관계 설명이 있으면 추가
                rel_str = f"- **{source}** → [{rtype}] → **{target}**"
                if desc:
                    rel_str += f": {desc}"
                context_parts.append(rel_str)

        # 3. Finnhub Real-time
        fh = all_data.get("finnhub", {})
        quote = fh.get("quote", {})
        if quote and "c" in quote:
            current = quote.get("c", 0)
            change = current - quote.get("pc", 0)
            pct = (change / quote.get("pc", 1) * 100) if quote.get("pc") else 0
            context_parts.append(
                f"\n---\n## 실시간 시세: ${current:.2f} ({'+' if change >= 0 else ''}{change:.2f}, {pct:.2f}%)"
            )

        metrics = fh.get("metrics", {}).get("metric", {})
        if metrics:
            context_parts.append(
                f"- P/E: {metrics.get('peBasicExclExtraTTM', 'N/A')}, P/B: {metrics.get('pbAnnual', 'N/A')}"
            )

        news = fh.get("news", [])
        if news:
            context_parts.append("\n---\n## 최근 뉴스 요약")
            for article in news[:3]:
                context_parts.append(f"- {article.get('headline', '')[:80]}")

        # 4. RAG Context (10-K)
        rag_text = all_data.get("rag_context", "")
        if rag_text:
            context_parts.append("\n---\n## 10-K 보고서 분석 내용")
            context_parts.append(rag_text)

        # 5. Earnings & Analyst Data (★ 정확도 개선 핵심)
        # 5-1. 실적 서프라이즈 (EPS)
        recs = fh.get("recommendations", [])
        if recs:
            latest_rec = recs[0]
            context_parts.append("\n---\n## 애널리스트 추천 트렌드")
            context_parts.append(
                f"- Buy: {latest_rec.get('buy', 0)}, Hold: {latest_rec.get('hold', 0)}, "
                f"Sell: {latest_rec.get('sell', 0)}, Strong Buy: {latest_rec.get('strongBuy', 0)}, "
                f"Strong Sell: {latest_rec.get('strongSell', 0)}"
            )
            context_parts.append(f"- 기준월: {latest_rec.get('period', 'N/A')}")

        # 5-2. 목표 주가
        target = fh.get("price_target", {})
        if target and "error" not in target and target.get("targetMean"):
            num_analysts = target.get("numberOfAnalysts", 0)
            context_parts.append(f"\n---\n## 애널리스트 목표 주가 ({num_analysts}명)")
            context_parts.append(
                f"- 평균: ${target['targetMean']:.2f}, "
                f"최고: ${target.get('targetHigh', 0):.2f}, "
                f"최저: ${target.get('targetLow', 0):.2f}"
            )

        # 5-3. 연간/분기 재무 데이터
        financials = all_data.get("financials", {})
        annual = financials.get("annual", [])
        if annual:
            context_parts.append("\n---\n## 연간 재무 데이터 (10-K 공시 기준)")
            for report in annual[:3]:
                year = report.get("fiscal_year", "N/A")
                parts_fin = [f"### {year}년"]
                for key, label in [
                    ("revenue", "매출"),
                    ("operating_income", "영업이익"),
                    ("net_income", "순이익"),
                    ("eps", "EPS"),
                    ("roe", "ROE"),
                    ("profit_margin", "영업이익률"),
                ]:
                    val = report.get(key)
                    if val is not None:
                        parts_fin.append(f"- {label}: {val}")
                context_parts.extend(parts_fin)

        quarterly = financials.get("quarterly", [])
        if quarterly:
            context_parts.append("\n---\n## 최근 분기 실적 (10-Q 공시 기준)")
            for report in quarterly[:2]:
                year = report.get("fiscal_year", "N/A")
                quarter = report.get("fiscal_quarter", "N/A")
                parts_fin = [f"### {year}년 {quarter}분기"]
                for key, label in [
                    ("revenue", "매출"),
                    ("operating_income", "영업이익"),
                    ("net_income", "순이익"),
                ]:
                    val = report.get(key)
                    if val is not None:
                        parts_fin.append(f"- {label}: {val}")
                context_parts.extend(parts_fin)

        # 6. News Sentiment (FinBERT)
        sentiments = all_data.get("news_sentiment", [])
        logger.info(
            f"[Context] FinBERT sentiment entries for {ticker}: {len(sentiments)}건"
        )
        if sentiments:
            context_parts.append(
                f"\n---\n## 🤖 AI 분석 최신 뉴스 심리 (FinBERT) - {len(sentiments)}건"
            )
            pos_count = sum(
                1
                for s in sentiments
                if str(s.get("sentiment_label", "")).lower() == "positive"
            )
            neg_count = sum(
                1
                for s in sentiments
                if str(s.get("sentiment_label", "")).lower() == "negative"
            )
            neu_count = len(sentiments) - pos_count - neg_count
            context_parts.append(
                f"📊 전체 감성 요약: 긍정 {pos_count}건 / 부정 {neg_count}건 / 중립 {neu_count}건"
            )
            for s in sentiments:
                news_ticker = s.get("ticker", "UNKNOWN")
                label = str(s.get("sentiment_label", "neutral")).upper()
                score = s.get("sentiment_score", 0)
                headline = s.get("headline", "")[:80]
                context_parts.append(
                    f"- [{news_ticker}] [{label} | 확신도: {score:.2f}] {headline}"
                )

        return "\n".join(context_parts) if context_parts else "추가 컨텍스트 없음"

    def _extract_tickers(self, query: str) -> List[str]:
        """Extract company tickers from user query using LLM"""
        # Pre-LLM: 한글 기업명을 먼저 직접 매핑 (LLM 환각 방지)
        try:
            from src.utils.ticker_resolver import COMPANY_MAP
        except ImportError:
            from utils.ticker_resolver import COMPANY_MAP

        pre_resolved = []
        remaining_query = query
        for name, ticker in COMPANY_MAP.items():
            if name in query.lower() and ticker not in pre_resolved:
                pre_resolved.append(ticker)

        # 매핑으로 모든 기업을 찾았으면 LLM 호출 생략
        if pre_resolved:
            logger.info(f"Pre-resolved tickers from COMPANY_MAP: {pre_resolved}")
            # 추가 기업이 있을 수 있으니 LLM도 호출하되, pre_resolved를 먼저 반환
            try:
                messages = [
                    {
                        "role": "system",
                        "content": "Extract ALL company ticker symbols mentioned in the query. You MUST return every single ticker found, comma-separated. Map Korean company names to their correct US stock ticker symbols. Examples: 애플->AAPL, 마이크로소프트->MSFT, 코카콜라->KO, 펩시->PEP, 펩시코->PEP, 테슬라->TSLA, 엔비디아->NVDA, 구글->GOOGL, 아마존->AMZN, 메타->META, 알파벳->GOOGL, 보잉->BA, 넷플릭스->NFLX, 세일즈포스->CRM, 일라이릴리->LLY, 버크셔해서웨이->BRK-B. Example output for '코카콜라와 펩시 비교해줘': KO,PEP. Example output for '애플 실적 알려줘': AAPL. Do NOT extract financial terms like AOCI, EBITDA, GAAP, USD. If no company is mentioned, return NOTHING.",
                    },
                    {"role": "user", "content": query},
                ]
                content = self._llm_chat(messages, temperature=0.0, max_tokens=30) or ""
                if content and "NOTHING" not in content:
                    llm_tickers = [
                        t.strip()
                        .replace(".", "")
                        .replace("'", "")
                        .replace('"', "")
                        .upper()
                        for t in content.split(",")
                        if t.strip()
                    ]
                    # LLM이 하이픈 없이 추출하는 경우 교정 (BRKA→BRK-A, BRKB→BRK-B, BFB→BF-B)
                    TICKER_ALIASES = {
                        "BRKA": "BRK-A",
                        "BRKB": "BRK-B",
                        "BRK.B": "BRK-B",
                        "BRK.A": "BRK-A",
                        "BFB": "BF-B",
                        "BFA": "BF-A",
                    }
                    for t in llm_tickers:
                        t = TICKER_ALIASES.get(t, t)  # 별칭 교정
                        if (
                            len(t) <= 6
                            and t.replace("-", "").isalpha()
                            and t.isascii()
                            and t not in pre_resolved
                        ):
                            pre_resolved.append(t)
            except Exception:
                pass
            return pre_resolved

        # 매핑 없으면 기존 LLM 방식
        try:
            messages = [
                {
                    "role": "system",
                    "content": "Extract ALL company ticker symbols mentioned in the query. You MUST return every single ticker found, comma-separated. Map Korean company names to their correct US stock ticker symbols. Examples: 애플->AAPL, 마이크로소프트->MSFT, 코카콜라->KO, 펩시->PEP, 펩시코->PEP, 테슬라->TSLA, 엔비디아->NVDA, 구글->GOOGL, 아마존->AMZN, 메타->META, 알파벳->GOOGL, 보잉->BA, 넷플릭스->NFLX, 세일즈포스->CRM, 일라이릴리->LLY. Example output for '코카콜라와 펩시 비교해줘': KO,PEP. Example output for '애플 실적 알려줘': AAPL. Do NOT extract financial terms like AOCI, EBITDA, GAAP, USD. If no company is mentioned, return NOTHING.",
                },
                {"role": "user", "content": query},
            ]
            content = self._llm_chat(messages, temperature=0.0, max_tokens=30) or ""
            if not content or "NOTHING" in content:
                return []

            tickers = [
                t.strip().replace(".", "").replace("'", "").replace('"', "").upper()
                for t in content.split(",")
                if t.strip()
            ]

            # Validation
            valid_tickers = []
            if self.finnhub:
                for t in tickers:
                    if len(t) <= 5 and t.isalpha() and t.isascii():
                        valid_tickers.append(t)

            return valid_tickers
        except Exception as e:
            logger.warning(f"Ticker extraction failed: {e}")
            return []

    def _resolve_ticker_name(self, input_text: str) -> Optional[str]:
        """Resolve Korean name or company name to Ticker"""
        if not input_text:
            return None

        # 긴 문장이 들어온 경우 (예: "애플 실적 어때?") DB 검색 건너뛰고 바로 LLM 추출로 넘김
        if len(input_text) > 20:
            extracted = self._extract_tickers(input_text)
            return extracted[0] if extracted else None

        # 1. Try Exact Ticker Match First (Prioritize "AAPL", "TSLA")
        # Even if input is "Apple", if we have a ticker "APPLE" (unlikely but possible), this checks.
        # Ideally, inputs like "AAPL" should hit this.
        try:
            res = (
                self.supabase.table("companies")
                .select("ticker")
                .eq("ticker", input_text.upper())
                .execute()
            )
            if res.data:
                return res.data[0]["ticker"]
        except Exception:
            pass

        # 2. Try Korean Name Match (e.g., "애플")
        try:
            res = (
                self.supabase.table("companies")
                .select("ticker")
                .ilike("korean_name", f"%{input_text}%")
                .execute()
            )
            if res.data:
                return res.data[0]["ticker"]
        except Exception:
            pass

        # 3. Try English Company Name Match (e.g., "Apple")
        try:
            res = (
                self.supabase.table("companies")
                .select("ticker")
                .ilike("company_name", f"%{input_text}%")
                .execute()
            )
            if res.data:
                return res.data[0]["ticker"]
        except Exception:
            pass

        # 4. Heuristic: If it looks like a ticker and we found nothing in DB, assume it might be a new ticker
        # But only if it's strictly a valid ticker format
        if input_text.isascii() and len(input_text) <= 5 and " " not in input_text:
            return input_text.upper()

        # 5. Fallback to LLM for short unresolved strings
        try:
            extracted = self._extract_tickers(input_text)
            return extracted[0] if extracted else input_text
        except Exception:
            return input_text

    def _register_company(self, ticker: str) -> str:
        """Register company to Supabase using Finnhub data"""
        if not self.finnhub:
            return "Finnhub 클라이언트가 설정되지 않았습니다."

        try:
            # Check if already exists
            existing = (
                self.supabase.table("companies")
                .select("ticker")
                .eq("ticker", ticker)
                .execute()
            )
            if existing.data:
                return f"이미 등록된 기업입니다: {ticker}"

            # Get profile
            profile = self.finnhub.get_company_profile(ticker)
            if not profile:
                return f"Finnhub에서 기업 정보를 찾을 수 없습니다: {ticker}"

            # Insert to Supabase
            data = {
                "ticker": ticker,
                "company_name": profile.get("name", ticker),
                "sector": profile.get("finnhubIndustry", "Unknown"),
                "industry": profile.get("finnhubIndustry", "Unknown"),
                "market_cap": profile.get("marketCapitalization", 0),
                "website": profile.get("weburl", ""),
                "description": f"Registered via Chatbot. {profile.get('name')} is a company in {profile.get('finnhubIndustry')} sector.",
            }

            # Generate Korean Name via LLM
            try:
                messages = [
                    {
                        "role": "system",
                        "content": "You are a translator. Return ONLY the Korean name for the company. No extra text.",
                    },
                    {
                        "role": "user",
                        "content": f"What is the common Korean name for '{profile.get('name')}' ({ticker})?",
                    },
                ]
                korean_name = self._llm_chat(messages, max_tokens=20).strip()
                data["korean_name"] = korean_name
            except Exception:
                pass

            self.supabase.table("companies").upsert(data).execute()
            logger.info(f"Registered company: {ticker} ({data.get('korean_name')})")
            return f"✅ 성공적으로 등록되었습니다: {profile.get('name')} ({ticker})\n한글명: {data.get('korean_name')}\n이제 이 기업에 대해 질문하거나 레포트를 생성할 수 있습니다."

        except Exception as e:
            logger.error(f"Registration failed: {e}")
            return f"등록 중 오류가 발생했습니다: {str(e)}"

    # _get_financial_data, _handle_tool_call_unified → chat_tools.ToolExecutor로 이동됨

    @traceable(run_type="chain", name="analyst_chat")
    def chat(
        self, message: str, ticker: Optional[str] = None, use_rag: bool = True
    ) -> Dict[str, Any]:
        """
        사용자 메시지를 처리하고 답변을 생성합니다. (리팩토링됨)
        """
        # 1. 도구(Tools) 로드 (별도 파일로 분리됨)
        try:
            from rag.chat_tools import get_chat_tools
        except ImportError:
            from src.rag.chat_tools import get_chat_tools

        tools = get_chat_tools()

        try:
            # 2. 티커 분석 및 컨텍스트 구축
            tickers = []
            if ticker:
                resolved = self._resolve_ticker_name(ticker)
                tickers = [resolved] if resolved else [ticker]
            else:
                tickers = self._extract_tickers(message)
                if tickers:
                    logger.info(f"Auto-extracted tickers from message: {tickers}")

            messages = [{"role": "system", "content": self.system_prompt}]
            messages.extend(self.conversation_history[-6:])

            context = ""
            if use_rag and tickers:
                context_parts = [self._build_context(message, t) for t in tickers]
                context = "\n\n---\n\n".join(context_parts)

            user_content = (
                f"[컨텍스트]\n{context}\n\n[질문]\n{message}" if context else message
            )
            # 멀티 티커일 때 비교 분석 명시 지시
            if len(tickers) > 1 and context:
                compare_hint = f"\n\n[중요 지시] 사용자가 여러 기업({', '.join(tickers)})을 언급했습니다. 각 기업의 뉴스 감정 분석과 재무 데이터를 모두 포함하여 비교 분석해주세요."
                user_content = (
                    f"[컨텍스트]\n{context}{compare_hint}\n\n[질문]\n{message}"
                )
            messages.append({"role": "user", "content": user_content})

            # 3. LLM 호출 (1차: 도구 사용 여부 결정)
            if self.llm_client:
                llm_result = self.llm_client.chat_completion_with_tools(
                    messages=messages,
                    tools=tools,
                    max_tokens=8192,
                    json_mode=True,
                )
            else:
                # OpenAI 폴백
                response = self.openai_client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    max_completion_tokens=8192,
                    response_format={"type": "json_object"},
                )
                resp_msg = response.choices[0].message
                llm_result = {
                    "content": resp_msg.content,
                    "tool_calls": (
                        [
                            {
                                "name": tc.function.name,
                                "arguments": json.loads(tc.function.arguments),
                                "id": tc.id,
                            }
                            for tc in resp_msg.tool_calls
                        ]
                        if resp_msg.tool_calls
                        else None
                    ),
                }

            tool_calls = llm_result.get("tool_calls")

            # 4. 도구 호출 처리
            chart_data = []
            recommendations = []

            if tool_calls:
                # 도구 결과를 메시지에 추가
                messages.append(
                    {
                        "role": "assistant",
                        "content": llm_result.get("content") or "도구를 호출합니다.",
                    }
                )
                for tc in tool_calls:
                    result = self.tool_executor.execute(tc)
                    messages.append(
                        {
                            "role": (
                                "tool"
                                if self.llm_client
                                and self.llm_client.provider != "gemini"
                                else "user"
                            ),
                            "name": tc["name"],
                            "content": f"[Tool Result: {tc['name']}]\n{result}",
                        }
                    )

                    # 차트 데이터 추출 (여러 티커 지원)
                    if tc["name"] == "get_stock_candles":
                        try:
                            parsed_res = json.loads(result)
                            if "error" not in parsed_res:
                                chart_data.append(parsed_res)
                        except Exception:
                            pass

                    # 도구 호출에서 티커가 발견되면 리스트에 추가 (레포트용)
                    args = tc.get("arguments", {})
                    if "ticker" in args and not tickers:
                        t = args["ticker"].upper()
                        if len(t) <= 5:
                            tickers.append(t)

                # 2차 LLM 호출 (최종 답변)
                raw_content = (
                    self._llm_chat(messages, max_tokens=8192, json_mode=True) or ""
                )
            else:
                raw_content = llm_result.get("content") or ""

            # JSON 파싱 및 최종 메시지 추출 (강화 버전)
            try:
                from utils.llm_parser import parse_llm_json_response
            except ImportError:
                from src.utils.llm_parser import parse_llm_json_response

            assistant_message, recommendations = parse_llm_json_response(raw_content)

            # 5. 레포트 생성 의도 파악 및 처리
            report_data, report_type = self._process_report_request(
                message, assistant_message, tickers
            )
            if report_data:
                assistant_message += f"\n\n(요청하신 분석 보고서를 {report_type.upper()}로 생성했습니다. 하단 버튼으로 다운로드하세요.)"

            # 6. 히스토리 업데이트 (답변 내용만 저장)
            self.conversation_history.append({"role": "user", "content": message})
            self.conversation_history.append(
                {"role": "assistant", "content": assistant_message}
            )

            return {
                "content": assistant_message,
                "report": report_data,
                "report_type": report_type,
                "tickers": tickers,
                "chart_data": chart_data,
                "recommendations": recommendations,  # 추천 질문 포함
                "context": context,  # 평가를 위한 컨텍스트 포함
            }

        except Exception as e:
            logger.error(f"Chat error: {e}")
            return {"content": f"오류 발생: {str(e)}", "report": None}

    @traceable(run_type="chain", name="analyst_chat_stream")
    def chat_stream(
        self, message: str, ticker: Optional[str] = None, use_rag: bool = True
    ):
        """
        사용자 메시지를 처리하고 스트리밍으로 답변을 생성합니다.
        (Yields dictionaries: {'type': 'chunk', 'content': '...'} or {'type': 'chart', 'data': ...} etc.)
        """
        try:
            from rag.chat_tools import get_chat_tools
        except ImportError:
            from src.rag.chat_tools import get_chat_tools

        tools = get_chat_tools()

        try:
            tickers = []
            if ticker:
                resolved = self._resolve_ticker_name(ticker)
                tickers = [resolved] if resolved else [ticker]
            else:
                tickers = self._extract_tickers(message)
                if tickers:
                    logger.info(f"[Stream] Auto-extracted tickers: {tickers}")

            messages = [{"role": "system", "content": self.system_prompt}]
            messages.extend(self.conversation_history[-6:])

            context = ""
            if use_rag and tickers:
                context_parts = [self._build_context(message, t) for t in tickers]
                context = "\n\n---\n\n".join(context_parts)

            # 멀티 티커일 때 비교 분석 명시 지시
            compare_hint = ""
            if len(tickers) > 1:
                compare_hint = f"\n\n[중요 지시] 사용자가 여러 기업({', '.join(tickers)})을 언급했습니다. 각 기업의 뉴스 감정 분석과 재무 데이터를 모두 포함하여 비교 분석해주세요."
            user_content = (
                f"[컨텍스트]\n{context}{compare_hint}\n\n[질문]\n{message}"
                if context
                else message
            )
            messages.append({"role": "user", "content": user_content})

            if self.llm_client:
                llm_result = self.llm_client.chat_completion_with_tools(
                    messages=messages,
                    tools=tools,
                    max_tokens=8192,
                    json_mode=True,  # We still use json_mode for the FIRST call to determine tool use.
                )
            else:
                response = self.openai_client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    max_completion_tokens=8192,
                    response_format={"type": "json_object"},
                )
                resp_msg = response.choices[0].message
                llm_result = {
                    "content": resp_msg.content,
                    "tool_calls": (
                        [
                            {
                                "name": tc.function.name,
                                "arguments": json.loads(tc.function.arguments),
                                "id": tc.id,
                            }
                            for tc in resp_msg.tool_calls
                        ]
                        if resp_msg.tool_calls
                        else None
                    ),
                }

            tool_calls = llm_result.get("tool_calls")
            chart_data = []

            if tool_calls:
                assistant_msg = {
                    "role": "assistant",
                    "content": llm_result.get("content") or "",
                }

                # Append tool_calls property for OpenAI compatibility
                if not self.llm_client or self.llm_client.provider != "gemini":
                    assistant_msg["tool_calls"] = [
                        {
                            "type": "function",
                            "id": tc.get("id", f"call_{i}"),
                            "function": {
                                "name": tc.get("name"),
                                "arguments": json.dumps(tc.get("arguments")),
                            },
                        }
                        for i, tc in enumerate(tool_calls)
                    ]

                messages.append(assistant_msg)

                for i, tc in enumerate(tool_calls):
                    result = self.tool_executor.execute(tc)

                    if self.llm_client and self.llm_client.provider == "gemini":
                        messages.append(
                            {
                                "role": "user",
                                "content": f"[Tool Result: {tc['name']}]\n{result}",
                            }
                        )
                    else:
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tc.get("id", f"call_{i}"),
                                "name": tc["name"],
                                "content": result,
                            }
                        )

                    if tc["name"] == "get_stock_candles":
                        try:
                            parsed_res = json.loads(result)
                            if "error" not in parsed_res:
                                chart_data.append(parsed_res)
                                yield {"type": "chart", "data": parsed_res}
                        except Exception:
                            pass

                    args = tc.get("arguments", {})
                    if "ticker" in args and not tickers:
                        t = args["ticker"].upper()
                        if len(t) <= 5:
                            tickers.append(t)

            # Generate final response as stream (JSON mode is False because we are streaming)
            # We enforce the output to not be JSON for streaming to easily yield text chunks.
            # We instruct the model to just write markdown text.
            stream_messages = messages.copy()
            stream_messages.append(
                {
                    "role": "system",
                    "content": "IMPORTANT: Do NOT output JSON. Write your answer in markdown text. Do NOT include 'recommendations' block as JSON. Let your final text naturally conclude.",
                }
            )

            full_content = ""
            try:
                # LLM streaming response
                streamer = self._llm_chat_stream(stream_messages, max_tokens=8192)
                for chunk in streamer:
                    if chunk:
                        full_content += chunk
                        yield {"type": "chunk", "content": chunk}
            except Exception as e:
                logger.error(f"Streaming LLM error: {e}")
                yield {"type": "error", "content": str(e)}

            # Generate recommendations dynamically using a separate fast call
            try:
                rec_messages = [
                    {
                        "role": "system",
                        "content": 'Generate 3 short recommended follow-up questions in Korean based on the previous response. Output in JSON: {"recommendations": ["Q1", "Q2", "Q3"]}',
                    }
                ]
                rec_messages.append({"role": "user", "content": full_content})
                rec_res = self._llm_chat(rec_messages, json_mode=True, max_tokens=200)
                rec_json = json.loads(rec_res)
                recs = rec_json.get("recommendations", [])
                if recs:
                    yield {"type": "recommendations", "data": recs}
            except Exception:
                pass

            # Report handling - we generate reports asynchronously or statically, but stream the link
            report_data, report_type = self._process_report_request(
                message, full_content, tickers
            )
            if report_data:
                msg_append = f"\n\n(요청하신 분석 보고서를 {report_type.upper()}로 생성했습니다. 하단 버튼으로 다운로드하세요.)"
                full_content += msg_append
                yield {"type": "chunk", "content": msg_append}
                # To actually send pdf_bytes through SSE is not ideal, we should skip PDF creation in stream and rely on the standalone report generator download endpoint,
                # BUT since it returns bytes, we can omit sending the bytes in the SSE stream itself, just let the User know they can use the report generation UI.

            self.conversation_history.append({"role": "user", "content": message})
            self.conversation_history.append(
                {"role": "assistant", "content": full_content}
            )
            yield {"type": "done"}

        except Exception as e:
            logger.error(f"Chat stream error: {e}")
            yield {"type": "error", "content": f"오류 발생: {str(e)}"}

    def _process_report_request(
        self, message: str, assistant_message: str, tickers: List[str]
    ):
        """레포트 생성 요청 여부를 확인하고 실행합니다."""
        keywords = [
            "레포트",
            "보고서",
            "다운로드",
            "파일",
            "report",
            "자료",
            "pdf",
            "피디에프",
        ]
        if not any(k in message.lower() for k in keywords):
            return None, "md"

        # target_tickers 초기화 (입력받은 tickers 사용)
        target_tickers = tickers if tickers else []

        # 히스토리에서 티커 역추적 (User 메시지 우선)
        if not target_tickers:
            for hist_msg in reversed(self.conversation_history):
                # 사용자가 직접 언급한 순서를 따르기 위해 user 메시지 우선 확인
                if hist_msg.get("role") == "user":
                    matches = re.findall(r"\b[A-Z]{2,5}\b", hist_msg["content"])
                    if matches:
                        # 사용자가 "A와 B 비교해줘"라고 했다면 matches=[A, B]
                        target_tickers = matches
                        break

            # User 메시지에서 못 찾았다면 Assistant 메시지에서 확인 (Fallback)
            if not target_tickers:
                for hist_msg in reversed(self.conversation_history):
                    if hist_msg.get("role") == "assistant":
                        matches = re.findall(r"\b[A-Z]{2,5}\b", hist_msg["content"])
                        if matches:
                            target_tickers = matches
                            break

        if not target_tickers:
            return None, "md"

        try:
            from rag.report_generator import ReportGenerator
            from utils.pdf_utils import create_pdf
            from utils.chart_utils import (
                generate_line_chart,
                generate_candlestick_chart,
                generate_volume_chart,
                generate_financial_chart,
            )

            generator = ReportGenerator()
            report_md = ""

            # --- 비교 분석 레포트 (2개 이상) ---
            if len(target_tickers) > 1:
                # 비교 분석 리포트 생성
                report_md = generator.generate_comparison_report(target_tickers)

                # 비교 분석용 차트 생성 (Line, Volume, Financial)
                chart_buffers = []
                try:
                    c1 = generate_line_chart(target_tickers)
                    if c1:
                        chart_buffers.append(c1)

                    c2 = generate_volume_chart(target_tickers)
                    if c2:
                        chart_buffers.append(c2)

                    c3 = generate_financial_chart(target_tickers)
                    if c3:
                        chart_buffers.append(c3)
                except Exception as e:
                    logger.warning(f"Comparison charts generation failed: {e}")

                # PDF 생성
                try:
                    pdf_bytes = create_pdf(report_md, chart_images=chart_buffers)
                    return pdf_bytes, "pdf"
                except Exception:
                    return report_md, "md"

            # --- 단일 기업 분석 레포트 ---
            else:
                target_ticker = target_tickers[0]

                # 1. Generate Report Content
                report_md = generator.generate_report(target_ticker)

                # 2. Generate All Charts
                chart_buffers = []
                try:
                    # Line Chart
                    c1 = generate_line_chart([target_ticker])
                    if c1:
                        chart_buffers.append(c1)

                    # Candlestick
                    c2 = generate_candlestick_chart([target_ticker])
                    if c2:
                        chart_buffers.append(c2)

                    # Volume
                    c3 = generate_volume_chart([target_ticker])
                    if c3:
                        chart_buffers.append(c3)

                    # Financial
                    c4 = generate_financial_chart([target_ticker])
                    if c4:
                        chart_buffers.append(c4)
                except Exception as e:
                    logger.warning(f"Chart generation failed: {e}")

                # 3. Create PDF with Charts
                try:
                    pdf_bytes = create_pdf(report_md, chart_images=chart_buffers)
                    return pdf_bytes, "pdf"
                except Exception:
                    return report_md, "md"

        except Exception as e:
            logger.warning(f"Report generation failed: {e}")
            return None, "md"

    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
        logger.info("Conversation history cleared")


if __name__ == "__main__":
    print("🔄 AnalystChatbot 초기화 중...")
    try:
        chatbot = AnalystChatbot()
        print(f"✅ 초기화 성공!")
        print(f"   Model: {chatbot.model}")

    except Exception as e:
        print(f"❌ 오류: {e}")
