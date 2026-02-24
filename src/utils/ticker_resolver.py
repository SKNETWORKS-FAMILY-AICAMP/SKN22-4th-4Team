"""
Ticker Resolver Utility
Ported from legacy Streamlit helper to support Django web app.
Handles resolution of company names (Korean/English) to valid stock tickers.
"""

# 기업명 매핑 테이블 (자주 사용되는 주요 기업)
COMPANY_MAP = {
    "apple": "AAPL",
    "aapl": "AAPL",
    "애플": "AAPL",
    "tesla": "TSLA",
    "tsla": "TSLA",
    "테슬라": "TSLA",
    "nvidia": "NVDA",
    "nvda": "NVDA",
    "엔비디아": "NVDA",
    "microsoft": "MSFT",
    "msft": "MSFT",
    "마이크로소프트": "MSFT",
    "google": "GOOGL",
    "googl": "GOOGL",
    "구글": "GOOGL",
    "알파벳": "GOOGL",
    "alphabet": "GOOGL",
    "amazon": "AMZN",
    "amzn": "AMZN",
    "아마존": "AMZN",
    "meta": "META",
    "메타": "META",
    "페이스북": "META",
    "netflix": "NFLX",
    "넷플릭스": "NFLX",
}


def resolve_to_ticker(term: str) -> tuple[str, str | None]:
    """한글명이나 영문명을 티커로 변환 (공용 함수)

    Args:
        term (str): 검색할 기업명 또는 티커

    Returns:
        tuple: (ticker, reason)
        reason은 웹 검색으로 찾은 경우에만 제공됨 (UI 표시용)
    """
    term = term.strip()

    # 1. 이미 티커 형식이면 반환
    if term.isupper() and term.isalpha() and len(term) <= 5:
        return term, None

    # 2. 매핑 테이블에서 검색
    lower_term = term.lower()
    if lower_term in COMPANY_MAP:
        return COMPANY_MAP[lower_term], None

    # 3. DB에서 검색 (Search API 활용)
    try:
        from src.data.supabase_client import SupabaseClient

        if SupabaseClient:
            df = SupabaseClient.search_companies(term)
            if df is not None and not df.empty:
                return df.iloc[0]["ticker"], None
    except Exception:
        pass

    # 4. Web Search Fallback (Tavily)
    try:
        from src.utils.ticker_search_agent import find_ticker_from_web

        # 너무 긴 텍스트는 검색 제외
        if len(term) < 20:
            found_ticker, reason = find_ticker_from_web(term)
            if found_ticker != "UNKNOWN":
                return found_ticker, reason
    except Exception as e:
        print(f"Web search failed: {e}")

    # 실패 시 대문자로 변환하여 티커로 가정하고 반환
    return term.upper(), None
