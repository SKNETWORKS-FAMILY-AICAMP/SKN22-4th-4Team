"""
Stock API Client - 실시간 주가, 뉴스, SEC 공시 데이터
Finnhub API + yfinance fallback 지원
"""

import os
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from django.core.cache import cache
from functools import wraps

load_dotenv()

logger = logging.getLogger(__name__)


def api_cache(timeout=300):
    """API 호출 결과를 캐싱하는 데코레이터"""

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # 캐시 키 생성
            key_parts = (
                [func.__name__]
                + [str(a) for a in args]
                + [f"{k}={v}" for k, v in kwargs.items()]
            )
            cache_key = "_".join(key_parts).replace(" ", "_")

            # 1. 캐시 확인
            cached_data = cache.get(cache_key)
            if cached_data is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_data

            # 2. 캐시 미스 시 원본 함수 호출
            result = func(self, *args, **kwargs)

            # 3. 에러 결과가 아니면 캐시 저장
            if result and not (isinstance(result, dict) and "error" in result):
                cache.set(cache_key, result, timeout=timeout)
                logger.debug(f"Cache saved for {cache_key}")

            return result

        return wrapper

    return decorator


# 0227mj


class StockAPIClient:
    """
    Stock API 클라이언트 (Finnhub + yfinance)
    - 실시간/과거 주가
    - 회사 뉴스
    - SEC 공시
    - 기업 프로필
    - 재무제표
    - yfinance fallback 지원
    """

    BASE_URL = "https://finnhub.io/api/v1"
    FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: str = None):
        """Initialize Stock API client"""
        self.api_key = api_key or os.getenv("FINNHUB_API_KEY")
        self.fmp_api_key = os.getenv("FMP_API_KEY")

        if self.api_key:
            self.api_key = self.api_key.strip()

        if self.fmp_api_key:
            self.fmp_api_key = self.fmp_api_key.strip()

        if not self.api_key or self.api_key == "your_finnhub_api_key_here":
            logger.warning(
                "FINNHUB_API_KEY not set. Get free key at https://finnhub.io"
            )
            self.api_key = None

        if not self.fmp_api_key:
            logger.debug("FMP_API_KEY not set. Earnings calendar unavailable.")

        self.session = requests.Session()

    def _request(self, endpoint: str, params: dict = None) -> Optional[Dict]:
        """Make API request"""
        if not self.api_key:
            return {"error": "Finnhub API key not configured"}

        params = params or {}
        params["token"] = self.api_key

        try:
            response = self.session.get(
                f"{self.BASE_URL}/{endpoint}", params=params, timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.warning(
                    f"Finnhub API 403 Forbidden (Premium endpoint?): {endpoint}"
                )
                return {
                    "error": "Prediction/Premium endpoint not available on this plan"
                }
            logger.error(f"Finnhub API error: {e}")
            return {"error": str(e)}
        except requests.exceptions.RequestException as e:
            logger.error(f"Finnhub API error: {e}")
            return {"error": str(e)}

    # ========== 주가 데이터 ==========

    def get_quote(self, symbol: str) -> Dict:
        """
        실시간 주가 조회
        Returns: c(현재가), h(고가), l(저가), o(시가), pc(전일종가), t(시간)
        Finnhub 실패 시 yfinance로 fallback
        """
        # Finnhub 시도
        result = self._request("quote", {"symbol": symbol.upper()})

        # Finnhub 성공 시 반환 (c 값이 0보다 큰 경우)
        if result and result.get("c", 0) > 0:
            return result

        # yfinance fallback
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol.upper())
            info = ticker.info

            if not info or "symbol" not in info:
                return {"error": "주가 데이터를 가져오지 못했습니다.", "c": 0}

            current_price = info.get("currentPrice") or info.get(
                "regularMarketPrice", 0
            )
            prev_close = info.get("previousClose", 0)

            return {
                "c": current_price,
                "h": info.get("dayHigh", 0),
                "l": info.get("dayLow", 0),
                "o": info.get("open", 0),
                "pc": prev_close,
                "t": int(datetime.now().timestamp()),
                "source": "yfinance",
            }
        except Exception as e:
            logger.error(f"yfinance quote fallback failed: {e}")
            return {"error": "주가 데이터를 가져오지 못했습니다.", "c": 0}

    def get_candles(
        self,
        symbol: str,
        resolution: str = "D",  # 1, 5, 15, 30, 60, D, W, M
        from_date: datetime = None,
        to_date: datetime = None,
    ) -> Dict:
        """
        캔들 차트 데이터 (OHLCV)
        resolution: 1=1분, 5=5분, D=일봉, W=주봉, M=월봉
        Finnhub 실패 시 yfinance로 fallback
        """
        to_date = to_date or datetime.now()
        from_date = from_date or (to_date - timedelta(days=30))

        # Finnhub 시도
        result = self._request(
            "stock/candle",
            {
                "symbol": symbol.upper(),
                "resolution": resolution,
                "from": int(from_date.timestamp()),
                "to": int(to_date.timestamp()),
            },
        )

        # Finnhub 성공 시 반환
        if result and result.get("s") == "ok":
            return result

        # yfinance fallback
        try:
            import yfinance as yf

            # resolution을 yfinance period로 변환
            days = (to_date - from_date).days
            period = f"{days}d" if days <= 60 else "3mo"

            ticker = yf.Ticker(symbol.upper())
            hist = ticker.history(period=period)

            if hist.empty:
                return {"error": "주가 데이터를 가져오지 못했습니다."}

            # Finnhub 형식으로 변환 (c, h, l, o, v, t)
            return {
                "s": "ok",
                "c": hist["Close"].tolist(),
                "h": hist["High"].tolist(),
                "l": hist["Low"].tolist(),
                "o": hist["Open"].tolist(),
                "v": hist["Volume"].tolist(),
                "t": [int(d.timestamp()) for d in hist.index],
            }
        except Exception as e:
            logger.error(f"yfinance fallback failed: {e}")
            return {"error": "주가 데이터를 가져오지 못했습니다."}

    # ========== 기업 정보 ==========
    # api_cache 추가
    @api_cache(timeout=86400)  # 24시간
    def get_company_profile(self, symbol: str) -> Dict:
        """기업 프로필 조회"""
        return self._request("stock/profile2", {"symbol": symbol.upper()})

    @api_cache(timeout=86400)  # 24시간
    def get_company_peers(self, symbol: str) -> List[str]:
        """경쟁사/유사기업 목록"""
        result = self._request("stock/peers", {"symbol": symbol.upper()})
        return result if isinstance(result, list) else []

    # ========== 뉴스 ==========

    @api_cache(timeout=900)  # 15분
    def get_company_news(
        self, symbol: str, from_date: str = None, to_date: str = None  # YYYY-MM-DD
    ) -> List[Dict]:
        """
        기업 관련 뉴스 조회
        Returns: headline, summary, source, url, datetime
        """
        to_date = to_date or datetime.now().strftime("%Y-%m-%d")
        from_date = from_date or (datetime.now() - timedelta(days=7)).strftime(
            "%Y-%m-%d"
        )

        result = self._request(
            "company-news", {"symbol": symbol.upper(), "from": from_date, "to": to_date}
        )
        return result if isinstance(result, list) else []

    @api_cache(timeout=900)  # 15분
    def get_market_news(self, category: str = "general") -> List[Dict]:
        """
        시장 전체 뉴스
        category: general, forex, crypto, merger
        """
        result = self._request("news", {"category": category})
        return result if isinstance(result, list) else []

    # ========== SEC 공시 ==========

    @api_cache(timeout=3600)  # 1시간
    def get_sec_filings(
        self,
        symbol: str = None,
        cik: str = None,
        form: str = None,  # 10-K, 10-Q, 8-K 등
        from_date: str = None,
        to_date: str = None,
    ) -> List[Dict]:
        """
        SEC 공시 목록 조회
        Returns: accessNumber, symbol, form, filedDate, acceptedDate, reportUrl
        """
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        if cik:
            params["cik"] = cik
        if form:
            params["form"] = form
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        result = self._request("stock/filings", params)
        return result if isinstance(result, list) else []

    # ========== 재무 데이터 ==========

    @api_cache(timeout=86400)  # 24시간
    def get_basic_financials(self, symbol: str, metric: str = "all") -> Dict:
        """
        기본 재무 지표
        metric: all, price, valuation, margin, profitability 등
        Finnhub 실패 시 yfinance로 fallback
        """
        # Finnhub 시도
        result = self._request(
            "stock/metric", {"symbol": symbol.upper(), "metric": metric}
        )

        # Finnhub 성공 시 반환
        if result and "metric" in result and result["metric"]:
            return result

        # yfinance fallback
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol.upper())
            info = ticker.info

            if not info or "symbol" not in info:
                return {"error": "재무 지표를 가져오지 못했습니다."}

            return {
                "symbol": symbol.upper(),
                "metric": {
                    "peBasicExclExtraTTM": info.get("trailingPE"),
                    "peExclExtraHighTTM": info.get("forwardPE"),
                    "pbAnnual": info.get("priceToBook"),
                    "roeRfy": (
                        (info.get("returnOnEquity", 0) or 0) * 100
                        if info.get("returnOnEquity")
                        else None
                    ),
                    "roaRfy": (
                        (info.get("returnOnAssets", 0) or 0) * 100
                        if info.get("returnOnAssets")
                        else None
                    ),
                    "dividendYieldIndicatedAnnual": (
                        (info.get("dividendYield", 0) or 0) * 100
                        if info.get("dividendYield")
                        else None
                    ),
                    "marketCapitalization": info.get("marketCap"),
                    "52WeekHigh": info.get("fiftyTwoWeekHigh"),
                    "52WeekLow": info.get("fiftyTwoWeekLow"),
                    "beta": info.get("beta"),
                },
                "source": "yfinance",
            }
        except Exception as e:
            logger.error(f"yfinance financials fallback failed: {e}")
            return {"error": "재무 지표를 가져오지 못했습니다."}

    @api_cache(timeout=86400)  # 24시간
    def get_financials_reported(self, symbol: str, freq: str = "annual") -> Dict:
        """
        실제 보고된 재무제표 데이터
        freq: annual, quarterly
        """
        return self._request(
            "stock/financials-reported", {"symbol": symbol.upper(), "freq": freq}
        )

    @api_cache(timeout=86400)  # 24시간
    def get_earnings(self, symbol: str) -> List[Dict]:
        """실적 발표 데이터 (EPS)"""
        result = self._request("stock/earnings", {"symbol": symbol.upper()})
        return result if isinstance(result, list) else []

    # ========== 추천/분석 ==========

    @api_cache(timeout=86400)  # 24시간
    def get_recommendation_trends(self, symbol: str) -> List[Dict]:
        """애널리스트 추천 트렌드 (Buy/Hold/Sell)"""
        result = self._request("stock/recommendation", {"symbol": symbol.upper()})
        return result if isinstance(result, list) else []

    @api_cache(timeout=86400)  # 24시간
    def get_price_target(self, symbol: str) -> Dict:
        """
        목표 주가 (애널리스트 컨센서스)
        Finnhub 실패 시 yfinance로 fallback
        """
        # Finnhub 시도
        result = self._request("stock/price-target", {"symbol": symbol.upper()})

        # Finnhub 성공 시 반환
        if result and "error" not in result:
            return result

        # yfinance fallback
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol.upper())
            info = ticker.info

            return {
                "symbol": symbol.upper(),
                "targetHigh": info.get("targetHighPrice"),
                "targetLow": info.get("targetLowPrice"),
                "targetMean": info.get("targetMeanPrice"),
                "targetMedian": info.get("targetMedianPrice"),
                "lastUpdated": datetime.now().strftime("%Y-%m-%d"),
                "numberOfAnalysts": info.get("numberOfAnalystOpinions", 0),
            }
        except Exception as e:
            logger.error(f"yfinance fallback failed: {e}")
            return {"error": "목표주가 데이터를 가져오지 못했습니다."}

    @api_cache(timeout=86400)  # 24시간
    def get_earnings_surprises(self, symbol: str) -> List[Dict]:
        """실적 서프라이즈 데이터"""
        result = self._request("stock/earnings", {"symbol": symbol.upper()})
        return result if isinstance(result, list) else []

    # ========== 캘린더 (FMP) ==========

    @api_cache(timeout=86400)  # 24시간
    def get_earnings_calendar(
        self, from_date: str = None, to_date: str = None
    ) -> List[Dict]:
        """
        실적 발표 캘린더 (FMP API 사용)
        Returns: list of dictionary
        """
        if not self.fmp_api_key:
            logger.warning("FMP API Key가 없어 캘린더 조회 불가")
            return []

        if not from_date:
            from_date = datetime.now().strftime("%Y-%m-%d")
        if not to_date:
            to_date = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")

        try:
            url = f"{self.FMP_BASE_URL}/earning_calendar"
            params = {
                "from": from_date,
                "to": to_date,
                "apikey": self.fmp_api_key,
            }
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            if isinstance(data, list):
                return data
            return []

        except Exception as e:
            logger.error(f"FMP Earnings Calendar API error: {e}")
            return []

    # ========== 유틸리티 ==========

    def format_quote_summary(self, symbol: str) -> str:
        """주가 정보를 읽기 쉬운 텍스트로 변환"""
        quote = self.get_quote(symbol)

        if "error" in quote:
            return f"주가 조회 실패: {quote['error']}"

        current = quote.get("c", 0)
        prev_close = quote.get("pc", 0)
        change = current - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0

        arrow = "📈" if change >= 0 else "📉"

        return f"""
{arrow} **{symbol.upper()}** 실시간 시세
- 현재가: ${current:.2f}
- 변동: {'+' if change >= 0 else ''}{change:.2f} ({'+' if change_pct >= 0 else ''}{change_pct:.2f}%)
- 고가: ${quote.get('h', 0):.2f} / 저가: ${quote.get('l', 0):.2f}
- 전일종가: ${prev_close:.2f}
""".strip()

    def format_news_summary(self, symbol: str, limit: int = 5) -> str:
        """최근 뉴스를 읽기 쉬운 텍스트로 변환"""
        news = self.get_company_news(symbol)[:limit]

        if not news:
            return f"{symbol.upper()} 관련 최근 뉴스가 없습니다."

        lines = [f"📰 **{symbol.upper()}** 최근 뉴스"]
        for i, article in enumerate(news, 1):
            headline = article.get("headline", "제목 없음")
            source = article.get("source", "")
            dt = datetime.fromtimestamp(article.get("datetime", 0))
            lines.append(
                f"{i}. [{headline}]({article.get('url', '#')}) - {source} ({dt.strftime('%m/%d')})"
            )

        return "\n".join(lines)


# 싱글톤 인스턴스
_client = None


def get_stock_api_client() -> StockAPIClient:
    """Get or create Stock API client singleton"""
    global _client
    if _client is None:
        _client = StockAPIClient()
    return _client


# 하위 호환성을 위한 별칭
FinnhubClient = StockAPIClient
get_finnhub_client = get_stock_api_client


if __name__ == "__main__":
    print("🔄 Stock API 클라이언트 테스트...")

    client = StockAPIClient()

    if client.api_key:
        print("✅ API 키 설정됨")

        # 테스트: Apple 주가
        print("\n📈 AAPL 주가:")
        print(client.format_quote_summary("AAPL"))

        # 테스트: 기업 프로필
        print("\n🏢 기업 프로필:")
        profile = client.get_company_profile("AAPL")
        print(f"  회사명: {profile.get('name')}")
        print(f"  산업: {profile.get('finnhubIndustry')}")
        print(f"  시가총액: ${profile.get('marketCapitalization', 0):,.0f}M")

    else:
        print("⚠️ FINNHUB_API_KEY가 설정되지 않았습니다.")
        print("   무료 API 키 발급: https://finnhub.io")
