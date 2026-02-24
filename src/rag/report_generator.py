"""
Report Generator - 구조화된 투자 분석 레포트 생성
Uses Gemini 2.5 Flash (or OpenAI fallback)
"""

import os
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

try:
    from src.rag.rag_base import RAGBase, logger
except ImportError:
    from rag.rag_base import RAGBase, logger

# Prompts directory
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


class ReportGenerator(RAGBase):
    """
    투자 분석 레포트 생성기
    Gemini 2.5 Flash 사용 (OpenAI fallback)
    """

    def __init__(self):
        """Initialize report generator inheriting from RAGBase"""
        report_model = os.getenv(
            "REPORT_MODEL", os.getenv("CHAT_MODEL", "gemini-2.5-flash")
        )
        super().__init__(model_name=report_model)

        # Load system prompt
        self.system_prompt = self._load_prompt("report_generator.txt")

        logger.info("ReportGenerator initialized (inherited from RAGBase)")

    def _get_company_data(self, ticker: str) -> Dict:
        """기업 데이터를 병렬로 수집 (DataRetriever 활용)"""
        if not self.data_retriever:
            # Fallback (최소한의 데이터만 수집)
            return {
                "company": None,
                "annual_reports": [],
                "quarterly_reports": [],
                "relationships": [],
                "stock_prices": [],
                "rag_context": "",
            }

        raw_data = self.data_retriever.get_company_context_parallel(
            ticker, include_finnhub=False, include_rag=True
        )

        # 레포트 포맷에 맞게 데이터 재구성
        return {
            "company": raw_data.get("company"),
            "annual_reports": raw_data.get("financials", {}).get("annual", []),
            "quarterly_reports": raw_data.get("financials", {}).get("quarterly", []),
            "relationships": raw_data.get("relationships", []),
            "stock_prices": raw_data.get("financials", {}).get("prices", []),
            "rag_context": raw_data.get("rag_context", ""),
        }

    def _format_data_context(self, data: Dict) -> str:
        """Format company data into context string with source labels"""
        from datetime import datetime
        import pytz

        parts = []
        kst = pytz.timezone("Asia/Seoul")
        now_kst = datetime.now(kst).strftime("%Y-%m-%d %H:%M KST")

        company = data.get("company")
        if company:
            parts.append(
                f"## 기업 개요 [Source: Supabase DB | 최종 업데이트: DB 동기화 기준]"
            )
            parts.append(f"- 회사명: {company.get('company_name', 'N/A')}")
            parts.append(f"- 티커: {company.get('ticker', 'N/A')}")
            parts.append(f"- 섹터: {company.get('sector', 'N/A')}")
            parts.append(f"- 산업: {company.get('industry', 'N/A')}")
            parts.append(f"- 시가총액: {company.get('market_cap', 'N/A')}")
            parts.append(f"- 직원 수: {company.get('employees', 'N/A')}")

        annual = data.get("annual_reports", [])
        if annual:
            parts.append(
                f"\n## 연간 재무 데이터 [Source: Supabase DB | 10-K 공시 기준]"
            )
            for report in annual[:3]:
                year = report.get("fiscal_year", "N/A")
                parts.append(f"\n### {year}년")
                parts.append(f"- 매출: {report.get('revenue', 'N/A')}")
                parts.append(f"- 영업이익: {report.get('operating_income', 'N/A')}")
                parts.append(f"- 순이익: {report.get('net_income', 'N/A')}")
                parts.append(f"- EPS: {report.get('eps', 'N/A')}")
                parts.append(f"- ROE: {report.get('roe', 'N/A')}")
                parts.append(f"- 영업이익률: {report.get('profit_margin', 'N/A')}")

        quarterly = data.get("quarterly_reports", [])
        if quarterly:
            parts.append(f"\n## 최근 분기 실적 [Source: Supabase DB | 10-Q 공시 기준]")
            for report in quarterly[:2]:
                year = report.get("fiscal_year", "N/A")
                quarter = report.get("fiscal_quarter", "N/A")
                parts.append(f"\n### {year}년 {quarter}분기")
                parts.append(f"- 매출: {report.get('revenue', 'N/A')}")
                parts.append(f"- 영업이익: {report.get('operating_income', 'N/A')}")
                parts.append(f"- 순이익: {report.get('net_income', 'N/A')}")

        relationships = data.get("relationships", [])
        if relationships:
            parts.append(
                f"\n## 기업 관계 [Source: GraphRAG (Supabase) | 관계망 분석 기준]"
            )
            for rel in relationships[:5]:
                parts.append(
                    f"- {rel.get('source_company')} → [{rel.get('relationship_type')}] → {rel.get('target_company')}"
                )

        prices = data.get("stock_prices", [])
        if prices and prices[0]:
            latest = prices[0]
            parts.append(f"\n## 최근 주가 [Source: Supabase DB | 마지막 동기화 기준]")
            parts.append(f"- 날짜: {latest.get('price_date', 'N/A')}")
            parts.append(f"- 종가: {latest.get('close_price', 'N/A')}")
            parts.append(f"- P/E: {latest.get('pe_ratio', 'N/A')}")
            parts.append(f"- P/B: {latest.get('pb_ratio', 'N/A')}")

        rag_context = data.get("rag_context", "")
        if rag_context:
            parts.append(
                f"\n## 10-K 보고서 심층 내용 [Source: VectorDB (10-K RAG) | SEC 공시 기준]"
            )
            parts.append(rag_context)

        return "\n".join(parts) if parts else "데이터 없음"

    def _get_finnhub_data(self, ticker: str, raw_finnhub: Optional[Dict] = None) -> str:
        """Get real-time data from Finnhub (Refactored to use pre-fetched data)"""
        # Finnhub 없으면 yfinance 폴백 사용
        if not self.finnhub:
            return self._get_yfinance_fallback(ticker)

        # 만약 raw_finnhub가 없으면 직접 수집 (하위 호환성)
        if not raw_finnhub:
            if self.data_retriever:
                raw_data = self.data_retriever.get_company_context_parallel(
                    ticker, include_finnhub=True, include_rag=False
                )
                raw_finnhub = raw_data.get("finnhub", {})
            else:
                return self._get_yfinance_fallback(ticker)

        # raw_finnhub가 비어있으면 yfinance 폴백
        if not raw_finnhub or not raw_finnhub.get("quote"):
            return self._get_yfinance_fallback(ticker)

        parts = []
        try:
            # 타임스탬프 추가
            import pytz

            kst = pytz.timezone("Asia/Seoul")
            now_kst = datetime.now(kst).strftime("%Y-%m-%d %H:%M KST")

            # 1. Quote
            quote = raw_finnhub.get("quote", {})
            if quote and "c" in quote:
                current = quote.get("c", 0)
                prev_close = quote.get("pc", 0)
                change = current - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0
                parts.append(
                    f"## 실시간 시세 [Source: Finnhub API | 조회시간: {now_kst}]"
                )
                parts.append(f"- 현재가: ${current:.2f}")
                parts.append(
                    f"- 변동: {'+' if change >= 0 else ''}{change:.2f} ({'+' if change_pct >= 0 else ''}{change_pct:.2f}%)"
                )
                parts.append(
                    f"- 고가/저가: ${quote.get('h', 0):.2f} / ${quote.get('l', 0):.2f}"
                )

            metrics_data = raw_finnhub.get("metrics", {})
            if metrics_data and "metric" in metrics_data:
                m = metrics_data["metric"]
                parts.append(f"\n## 주요 재무 지표 [Source: Finnhub API | TTM 기준]")
                parts.append(f"- P/E (TTM): {m.get('peBasicExclExtraTTM', 'N/A')}")
                parts.append(f"- P/B: {m.get('pbAnnual', 'N/A')}")
                parts.append(f"- ROE: {m.get('roeRfy', 'N/A')}%")
                parts.append(
                    f"- 배당수익률: {m.get('dividendYieldIndicatedAnnual', 'N/A')}%"
                )

            recs = raw_finnhub.get("recommendations", [])
            if recs:
                latest = recs[0]
                parts.append(
                    f"\n## 애널리스트 의견 [Source: Finnhub API | 최신 컨센서스]"
                )
                parts.append(
                    f"- 추천: Buy({latest.get('buy', 0)}), Hold({latest.get('hold', 0)}), Sell({latest.get('sell', 0)})"
                )

            target = raw_finnhub.get("price_target", {})
            if target and "targetMean" in target:
                target_mean = target.get("targetMean")
                target_high = target.get("targetHigh")

                # None 체크 및 기본값 설정
                if target_mean is None:
                    target_mean = 0
                if target_high is None:
                    target_high = 0

                parts.append(
                    f"- 목표가 평균: ${target_mean:.2f} (최고 ${target_high:.2f})"
                )

            news = raw_finnhub.get("news", [])
            if news:
                parts.append(
                    f"\n## 최근 뉴스 요약 [Source: Finnhub API | 최근 3일 기사]"
                )
                for article in news[:3]:
                    headline = article.get("headline", "")[:70]
                    parts.append(f"- {headline}")

            peers = raw_finnhub.get("peers", [])
            if peers:
                parts.append(
                    f"\n## 주요 경쟁사 [Source: Finnhub API]: {', '.join(peers[:5])}"
                )

        except Exception as e:
            logger.warning(f"Formatting Finnhub data error: {e}")

        return "\n".join(parts)

    def _get_yfinance_fallback(self, ticker: str) -> str:
        """yfinance를 사용한 실시간 데이터 폴백"""
        try:
            import yfinance as yf
            import pytz

            kst = pytz.timezone("Asia/Seoul")
            now_kst = datetime.now(kst).strftime("%Y-%m-%d %H:%M KST")

            stock = yf.Ticker(ticker)
            info = stock.info

            if not info or "symbol" not in info:
                return ""

            parts = []
            parts.append(f"## 실시간 시세 [Source: yfinance | 조회시간: {now_kst}]")

            current_price = info.get("currentPrice") or info.get(
                "regularMarketPrice", 0
            )
            prev_close = info.get("previousClose", 0)
            if current_price and prev_close:
                change = current_price - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0
                parts.append(f"- 현재가: ${current_price:.2f}")
                parts.append(
                    f"- 변동: {'+' if change >= 0 else ''}{change:.2f} ({'+' if change_pct >= 0 else ''}{change_pct:.2f}%)"
                )

            day_high = info.get("dayHigh", 0)
            day_low = info.get("dayLow", 0)
            if day_high and day_low:
                parts.append(f"- 고가/저가: ${day_high:.2f} / ${day_low:.2f}")

            parts.append(f"\n## 주요 재무 지표 [Source: yfinance | TTM 기준]")
            parts.append(f"- P/E (TTM): {info.get('trailingPE', 'N/A')}")
            parts.append(f"- Forward P/E: {info.get('forwardPE', 'N/A')}")
            parts.append(f"- P/B: {info.get('priceToBook', 'N/A')}")

            roe = info.get("returnOnEquity")
            if roe:
                parts.append(f"- ROE: {roe * 100:.2f}%")

            div_yield = info.get("dividendYield")
            if div_yield:
                parts.append(f"- 배당수익률: {div_yield * 100:.2f}%")

            # 52주 고가/저가
            week52_high = info.get("fiftyTwoWeekHigh", 0)
            week52_low = info.get("fiftyTwoWeekLow", 0)
            if week52_high and week52_low:
                parts.append(f"\n## 52주 가격 범위 [Source: yfinance]")
                parts.append(f"- 52주 최고가: ${week52_high:.2f}")
                parts.append(f"- 52주 최저가: ${week52_low:.2f}")

            # 시가총액
            market_cap = info.get("marketCap", 0)
            if market_cap:
                if market_cap >= 1e12:
                    parts.append(f"- 시가총액: ${market_cap/1e12:.2f}T")
                elif market_cap >= 1e9:
                    parts.append(f"- 시가총액: ${market_cap/1e9:.2f}B")

            # 애널리스트 추천
            rec = info.get("recommendationKey")
            if rec:
                parts.append(f"\n## 애널리스트 의견 [Source: yfinance]")
                parts.append(f"- 추천: {rec.upper()}")
                target_mean = info.get("targetMeanPrice")
                target_high = info.get("targetHighPrice")
                if target_mean:
                    parts.append(f"- 목표가 평균: ${target_mean:.2f}")
                if target_high:
                    parts.append(f"- 목표가 최고: ${target_high:.2f}")

            logger.info(f"yfinance fallback used for {ticker}")
            return "\n".join(parts)

        except Exception as e:
            logger.warning(f"yfinance fallback failed for {ticker}: {e}")
            return ""

    def generate_report(self, ticker: str) -> str:
        """분석 레포트 생성 (병렬 수집 레이어 활용)"""
        try:
            # 1. 모든 데이터 통합 병렬 수집 (한 번의 네트워크 대기)
            if self.data_retriever:
                logger.info(f"Fetching all data for {ticker} in parallel...")
                all_data = self.data_retriever.get_company_context_parallel(ticker)

                # 레포트용 데이터 재구성
                db_data = {
                    "company": all_data.get("company"),
                    "annual_reports": all_data.get("financials", {}).get("annual", []),
                    "quarterly_reports": all_data.get("financials", {}).get(
                        "quarterly", []
                    ),
                    "relationships": all_data.get("relationships", []),
                    "stock_prices": all_data.get("financials", {}).get("prices", []),
                    "rag_context": all_data.get("rag_context", ""),
                }

                context = (
                    self._format_data_context(db_data) if db_data.get("company") else ""
                )
                finnhub_data = self._get_finnhub_data(
                    ticker, raw_finnhub=all_data.get("finnhub")
                )
            else:
                # 레거시 방식 (데이터가 없을 경우)
                data = self._get_company_data(ticker)
                context = self._format_data_context(data)
                finnhub_data = self._get_finnhub_data(ticker)

            # Combine contexts
            if context and finnhub_data:
                full_context = f"{context}\n\n---\n\n{finnhub_data}"
            elif finnhub_data:
                full_context = finnhub_data
            elif context:
                full_context = context
            else:
                return f"❌ '{ticker}' 데이터를 찾을 수 없습니다. Finnhub API 키를 확인하세요."

            # Generate report
            messages = [
                {"role": "system", "content": self.system_prompt},
                {
                    "role": "user",
                    "content": f"다음 데이터를 바탕으로 {ticker.upper()} 투자 분석 보고서를 작성해주세요.\n\n{full_context}",
                },
            ]

            report = None
            used_model = self.model

            # Generate report with selected model
            try:
                logger.info(f"Generating report with model: {self.model}")
                report = self._llm_chat(messages, max_tokens=3000)

                if not report:
                    raise ValueError("Empty response from model")

            except Exception as e:
                logger.warning(f"Model {self.model} failed: {e}. Retrying...")
                used_model = "fallback"
                try:
                    report = self._llm_chat(messages, max_tokens=3000, temperature=0.2)
                except Exception as e2:
                    logger.error(f"Retry failed: {e2}")
                    return f"❌ 레포트 생성 실패: {str(e2)}"

            if not report:
                return "❌ 레포트 생성 실패: 모델로부터 내용을 받아오지 못했습니다."

            # Add metadata
            header = f"""---
**생성일시**: {datetime.now().strftime('%Y-%m-%d %H:%M')}
**모델**: {used_model}
**티커**: {ticker}

---

"""
            return header + report

        except Exception as e:
            logger.error(f"Report generation error: {e}")
            return f"❌ 레포트 생성 중 오류가 발생했습니다: {str(e)}"

    def generate_comparison_report(self, tickers: list) -> str:
        """Generate comparison report for multiple companies"""
        try:
            context_parts = []

            for ticker in tickers:
                context_parts.append(f"\n# {ticker.upper()}")

                # Get Supabase data
                supabase_data = self._get_company_data(ticker)
                supabase_context = (
                    self._format_data_context(supabase_data)
                    if supabase_data.get("company")
                    else ""
                )

                # Get Finnhub data
                finnhub_context = self._get_finnhub_data(ticker)

                # Combine
                if supabase_context and finnhub_context:
                    context_parts.append(supabase_context)
                    context_parts.append("---")
                    context_parts.append(finnhub_context)
                elif finnhub_context:
                    context_parts.append(finnhub_context)
                elif supabase_context:
                    context_parts.append(supabase_context)
                else:
                    context_parts.append(f"⚠️ {ticker} 데이터 없음")

            full_context = "\n".join(context_parts)

            if not full_context.strip():
                return "❌ 비교할 회사 데이터를 찾을 수 없습니다."

            messages = [
                {"role": "system", "content": self.system_prompt},
                {
                    "role": "user",
                    "content": f"다음 기업들을 비교 분석하는 보고서를 작성해주세요: {', '.join(tickers)}\n\n{full_context}",
                },
            ]

            try:
                # Generate comparison report
                logger.info(f"Generating comparison report with model: {self.model}")
                content = self._llm_chat(messages, max_tokens=4000)
                if not content:
                    raise ValueError("Empty response from model")
                return content

            except Exception as e:
                logger.warning(f"Model {self.model} failed: {e}. Retrying...")
                try:
                    content = self._llm_chat(messages, max_tokens=4000, temperature=0.2)
                    if not content:
                        return "❌ 비교 보고서 생성 실패: 모델로부터 내용을 받아오지 못했습니다."
                    return content
                except Exception as e2:
                    return f"❌ 비교 보고서 생성 실패: {str(e2)}"

        except Exception as e:
            logger.error(f"Comparison report error: {e}")
            return f"❌ 비교 보고서 생성 중 오류 발생: {str(e)}"


if __name__ == "__main__":
    print("🔄 ReportGenerator 초기화 중...")

    try:
        generator = ReportGenerator()
        print(f"✅ 초기화 성공!")
        print(f"   Model: {generator.model}")
        print(f"   System Prompt: {len(generator.system_prompt)} chars")

        # Test report generation
        print("\n📝 테스트: AAPL 레포트 생성")
        report = generator.generate_report("AAPL")
        print(f"\n📊 레포트:\n{report[:500]}...")

    except Exception as e:
        print(f"❌ 오류: {e}")
