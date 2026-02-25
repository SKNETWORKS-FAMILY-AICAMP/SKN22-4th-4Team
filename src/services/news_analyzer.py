import os
import time
import logging
from datetime import datetime, timedelta
from typing import List

from src.utils.sentiment_analyzer import FinBERTSentimentAnalyzer
from src.data.supabase_client import get_supabase
from src.rag.graph_rag import GraphRAG
import finnhub

logger = logging.getLogger(__name__)


class NewsAnalyzerService:
    """
    Finnhub에서 주요 기업의 뉴스를 수집하고 FinBERT를 이용해 감성 분석 후 Supabase에 저장하는 서비스.
    Neo4j(GraphRAG)를 연동하여 연관 기업(공급망 등)의 뉴스도 함께 분석합니다.
    """

    def __init__(self):
        self.finnhub_api_key = os.getenv("FINNHUB_API_KEY")
        self.finnhub_client = None
        if self.finnhub_api_key and self.finnhub_api_key != "your_finnhub_api_key_here":
            self.finnhub_client = finnhub.Client(api_key=self.finnhub_api_key)

        self.supabase = get_supabase()
        self.analyzer = FinBERTSentimentAnalyzer()
        self.graph_rag = GraphRAG()

    def _get_base_tickers(self) -> List[str]:
        """Supabase에서 기본 분석 대상 기업 목록 조회"""
        try:
            res = self.supabase.table("companies").select("ticker").execute()
            if res.data:
                return [item["ticker"] for item in res.data if item.get("ticker")]
            return ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META"]
        except Exception as e:
            logger.warning(f"DB 접근 실패로 기본 티커를 사용합니다: {e}")
            return ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META"]

    def _expand_tickers_with_graphrag(self, base_tickers: List[str]) -> List[str]:
        """Neo4j를 이용해 기본 기업들과 연관된 핵심 기업 목록 확장"""
        logger.info("Neo4j에서 연관 기업(공급망 등) 정보를 가져옵니다...")
        extended_tickers = set(base_tickers)

        for ticker in base_tickers:
            try:
                rels = self.graph_rag.find_relationships(ticker)
                if rels:
                    for rel in rels.get("outgoing", []) + rels.get("incoming", []):
                        target = rel.get("target_company")
                        source = rel.get("source_company")

                        if (
                            target
                            and target != ticker
                            and len(target) <= 5
                            and target.isupper()
                        ):
                            extended_tickers.add(target)
                        if (
                            source
                            and source != ticker
                            and len(source) <= 5
                            and source.isupper()
                        ):
                            extended_tickers.add(source)
            except Exception as e:
                logger.warning(f"[{ticker}] 연관 기업 조회 실패: {e}")

        tickers_to_analyze = list(extended_tickers)
        logger.info(
            f"총 분석 대상 기업 수: {len(tickers_to_analyze)} (Base: {len(base_tickers)})"
        )
        return tickers_to_analyze

    def run_pipeline(self) -> int:
        """전체 뉴스 감성 분석 파이프라인 실행"""
        if not self.finnhub_client:
            logger.error("Finnhub API 키가 설정되지 않아 파이프라인을 중단합니다.")
            return 0

        logger.info("[START] 뉴스 감성 분석(FinBERT) 파이프라인 시작...")

        # 1. 대상 기업 수집 (Supabase + Neo4j)
        base_tickers = self._get_base_tickers()
        tickers_to_analyze = self._expand_tickers_with_graphrag(base_tickers)

        # 2. 날짜 설정 (최근 3일)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        total_inserted = 0

        # 3. 뉴스 수집 및 형태소/감성 분석 (Finnhub + FinBERT)
        for ticker in tickers_to_analyze:
            logger.info(f"[{ticker}] 뉴스 감성 분석 진행 중 ({start_str} ~ {end_str})")
            try:
                news_list = self.finnhub_client.company_news(
                    ticker, _from=start_str, to=end_str
                )

                # 최신순 5개만 샘플 처리
                news_list = news_list[:5]

                if not news_list:
                    continue

                inserted_count = 0
                for news in news_list:
                    news_id = news.get("id")
                    headline = news.get("headline", "")
                    summary = news.get("summary", "")

                    # 중복 저장 방지
                    existing = (
                        self.supabase.table("news_sentiment")
                        .select("id")
                        .eq("news_id", news_id)
                        .execute()
                    )
                    if existing.data:
                        continue  # 이미 분석됨

                    # 감성 분석
                    text_to_analyze = f"{headline}. {summary}"
                    sentiment_result = self.analyzer.analyze(text_to_analyze)

                    # Supabase 저장
                    record = {
                        "ticker": ticker,
                        "news_id": news_id,
                        "headline": headline,
                        "summary": summary,
                        "source": news.get("source", ""),
                        "url": news.get("url", ""),
                        "published_at": datetime.fromtimestamp(
                            news.get("datetime", 0)
                        ).isoformat(),
                        "sentiment_label": sentiment_result["label"],
                        "sentiment_score": sentiment_result["score"],
                    }

                    self.supabase.table("news_sentiment").insert(record).execute()
                    inserted_count += 1

                    # Finnhub API Rate Limit 정책 맞춤 (초당 2~3회 호출 제한 존재)
                    time.sleep(0.5)

                if inserted_count > 0:
                    logger.info(f"  [{ticker}] {inserted_count}건 저장 완료.")
                total_inserted += inserted_count

            except Exception as e:
                logger.error(f"[{ticker}] 뉴스 분석 중 에러 발생: {e}")

        logger.info(
            f"[DONE] 파이프라인 종료. 총 {total_inserted}건 데이터 분석/저장 완료."
        )
        return total_inserted
