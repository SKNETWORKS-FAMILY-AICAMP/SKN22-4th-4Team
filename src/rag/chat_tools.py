"""
AnalystChatbot에서 사용하는 도구(Tool) 정의, 스키마, 실행 로직 관리
"""

import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def get_chat_tools():
    """챗봇이 사용할 수 있는 도구 목록 반환 (OpenAI/Gemini 호환 형식)"""
    return [
        {
            "type": "function",
            "function": {
                "name": "get_stock_quote",
                "description": "Get real-time stock price (c), change (d), percent change (dp), high (h), low (l).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "Company ticker symbol (e.g. AAPL)",
                        }
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_company_profile",
                "description": "Get company profile (industry, market cap, IPO date, etc).",
                "parameters": {
                    "type": "object",
                    "properties": {"ticker": {"type": "string"}},
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_basic_financials",
                "description": "Get basic financial metrics (P/E, P/B, ROE, ROA, Market Cap, 52-week High/Low, beta).",
                "parameters": {
                    "type": "object",
                    "properties": {"ticker": {"type": "string"}},
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_company_earnings",
                "description": "Get recent earnings surprise history (estimated EPS vs actual EPS). **IMPORTANT: The 'period' and 'year' return Fiscal periods, which may be up to 1 year ahead of the calendar date for companies like Apple (e.g. FY2025 Q1 ends in Dec 2024 calendar time). Focus on the 'period' date to explain the actual timeline to users.**",
                "parameters": {
                    "type": "object",
                    "properties": {"ticker": {"type": "string"}},
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_price_target",
                "description": "Get analyst price target and consensus.",
                "parameters": {
                    "type": "object",
                    "properties": {"ticker": {"type": "string"}},
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_company_news",
                "description": "Get recent company news.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string"},
                        "from_date": {
                            "type": "string",
                            "description": "YYYY-MM-DD",
                        },
                        "to": {"type": "string", "description": "YYYY-MM-DD"},
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_market_news",
                "description": "Get general market news.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "enum": ["general", "forex", "crypto", "merger"],
                        }
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "register_company",
                "description": "Register a new company to the database if the user asks to add/register it.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "The company ticker symbol to register.",
                        }
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_exchange_rate",
                "description": "Get current exchange rate between currencies. Default is USD to KRW.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "from_currency": {
                            "type": "string",
                            "description": "Source currency code (e.g., USD, EUR)",
                        },
                        "to_currency": {
                            "type": "string",
                            "description": "Target currency code (e.g., KRW, JPY)",
                        },
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "convert_to_krw",
                "description": "Convert a USD amount to Korean Won.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "usd_amount": {
                            "type": "number",
                            "description": "Amount in USD to convert",
                        }
                    },
                    "required": ["usd_amount"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_stock_candles",
                "description": "Get historical stock price data (OHLCV) for charting.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "Company ticker symbol (e.g. AAPL)",
                        },
                        "resolution": {
                            "type": "string",
                            "description": "Candle resolution. D=Daily, W=Weekly, M=Monthly.",
                        },
                        "days": {
                            "type": "integer",
                            "description": "Number of days of history to fetch.",
                        },
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "add_to_favorites",
                "description": "Add a company to the user's favorites/watchlist.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "Company ticker symbol to add (e.g. AAPL).",
                        }
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "remove_from_favorites",
                "description": "Remove a company from the user's favorites/watchlist.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "Company ticker symbol to remove (e.g. AAPL).",
                        }
                    },
                    "required": ["ticker"],
                },
            },
        },
    ]


class ToolExecutor:
    """
    도구 호출 실행기 - analyst_chat.py에서 분리
    Finnhub, Exchange, Favorites 도구를 통합 실행합니다.
    """

    def __init__(self, finnhub=None, exchange_client=None, register_func=None):
        self.finnhub = finnhub
        self.exchange_client = exchange_client
        self._register_company = register_func

    def execute(self, tool_call: dict) -> str:
        """도구 호출 실행 (dict 형태: {name, arguments, id})"""
        name = tool_call.get("name", "")
        args = tool_call.get("arguments", {})
        if isinstance(args, str):
            args = json.loads(args)

        logger.info(f"Tool Call: {name} with {args}")

        try:
            handler = self._get_handler(name)
            if handler:
                return handler(args)
            return json.dumps({"error": f"Unknown function: {name}"})
        except Exception as e:
            logger.error(f"Error executing {name}: {e}")
            return json.dumps({"error": f"실행 중 오류: {str(e)}"})

    def _get_handler(self, name: str):
        """함수 이름에 따른 핸들러 매핑"""
        handlers = {
            "get_stock_quote": self._stock_quote,
            "get_company_profile": self._company_profile,
            "get_basic_financials": self._basic_financials,
            "get_company_earnings": self._company_earnings,
            "get_price_target": self._price_target,
            "get_company_news": self._company_news,
            "get_market_news": self._market_news,
            "register_company": self._register,
            "get_exchange_rate": self._exchange_rate,
            "convert_to_krw": self._convert_krw,
            "get_stock_candles": self._stock_candles,
            "add_to_favorites": self._add_favorite,
            "remove_from_favorites": self._remove_favorite,
        }
        return handlers.get(name)

    def _stock_quote(self, args):
        res = self.finnhub.get_quote(args.get("ticker"))
        return json.dumps(res, ensure_ascii=False)

    def _company_profile(self, args):
        res = self.finnhub.get_company_profile(args.get("ticker"))
        return json.dumps(res, ensure_ascii=False)

    def _basic_financials(self, args):
        res = self.finnhub.get_basic_financials(args.get("ticker"))
        return json.dumps(res, ensure_ascii=False)

    def _company_earnings(self, args):
        res = self.finnhub.get_earnings(args.get("ticker"))
        return json.dumps(res, ensure_ascii=False)

    def _price_target(self, args):
        res = self.finnhub.get_price_target(args.get("ticker"))
        return json.dumps(res, ensure_ascii=False)

    def _company_news(self, args):
        res = self.finnhub.get_company_news(
            args.get("ticker"), args.get("from_date"), args.get("to")
        )
        return json.dumps(res[:5], ensure_ascii=False)

    def _market_news(self, args):
        res = self.finnhub.get_market_news(args.get("category", "general"))
        return json.dumps(res[:5], ensure_ascii=False)

    def _register(self, args):
        if self._register_company:
            return self._register_company(args.get("ticker"))
        return json.dumps({"error": "등록 기능 미사용"})

    def _exchange_rate(self, args):
        if not self.exchange_client:
            return json.dumps({"error": "환율 서비스 비활성화"})
        from_curr = args.get("from_currency", "USD")
        to_curr = args.get("to_currency", "KRW")
        rate = self.exchange_client.get_rate(from_curr, to_curr)
        if rate:
            return json.dumps(
                {
                    "from": from_curr,
                    "to": to_curr,
                    "rate": rate,
                    "formatted": self.exchange_client.format_rate_for_display(
                        from_curr, to_curr, rate
                    ),
                },
                ensure_ascii=False,
            )
        return json.dumps({"error": "환율 조회 실패"})

    def _convert_krw(self, args):
        if not self.exchange_client:
            return json.dumps({"error": "환율 서비스 비활성화"})
        usd_amount = args.get("usd_amount", 0)
        krw_amount = self.exchange_client.convert(usd_amount, "USD", "KRW")
        rate = self.exchange_client.get_rate("USD", "KRW")
        if krw_amount and rate:
            return json.dumps(
                {
                    "usd_amount": usd_amount,
                    "krw_amount": krw_amount,
                    "rate": rate,
                    "formatted": f"${usd_amount:,.2f} = ₩{krw_amount:,.0f} (환율: {rate:,.2f}원/달러)",
                },
                ensure_ascii=False,
            )
        return json.dumps({"error": "변환 실패"})

    def _stock_candles(self, args):
        ticker = args.get("ticker", "").upper()
        resolution = args.get("resolution", "D")
        days = args.get("days", 30)

        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)

        # 1) Finnhub 시도
        res = self.finnhub.get_candles(ticker, resolution, from_date, to_date)
        if res and res.get("s") == "ok":
            res["ticker"] = ticker
            res["resolution"] = resolution
            return json.dumps(res, ensure_ascii=False)

        # 2) yfinance fallback (Finnhub 403 Premium 대응)
        try:
            import yfinance as yf

            period = f"{days}d" if days <= 60 else "3mo"
            yf_ticker = yf.Ticker(ticker)
            hist = yf_ticker.history(period=period)

            if hist.empty:
                return json.dumps(
                    {"error": "주가 데이터를 가져오지 못했습니다."}, ensure_ascii=False
                )

            logger.info(f"yfinance fallback used for candles: {ticker}")
            result = {
                "s": "ok",
                "c": hist["Close"].tolist(),
                "h": hist["High"].tolist(),
                "l": hist["Low"].tolist(),
                "o": hist["Open"].tolist(),
                "v": hist["Volume"].tolist(),
                "t": [int(d.timestamp()) for d in hist.index],
                "ticker": ticker,
                "resolution": resolution,
            }
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"yfinance candle fallback failed for {ticker}: {e}")

        return json.dumps(
            {"error": "주가 데이터를 가져오지 못했습니다."}, ensure_ascii=False
        )

    def _add_favorite(self, args):
        try:
            try:
                from tools.favorites_manager import add_to_favorites_tool
            except ImportError:
                from src.tools.favorites_manager import add_to_favorites_tool
            return add_to_favorites_tool(args.get("ticker", ""))
        except ImportError:
            return "시스템 오류: 즐겨찾기 모듈을 불러올 수 없습니다."
        except Exception as e:
            return f"오류 발생: {str(e)}"

    def _remove_favorite(self, args):
        try:
            try:
                from tools.favorites_manager import remove_from_favorites_tool
            except ImportError:
                from src.tools.favorites_manager import remove_from_favorites_tool
            return remove_from_favorites_tool(args.get("ticker", ""))
        except ImportError:
            return "시스템 오류: 즐겨찾기 모듈을 불러올 수 없습니다."
        except Exception as e:
            return f"오류 발생: {str(e)}"
