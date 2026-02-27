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
    "facebook": "META",
    "netflix": "NFLX",
    "넷플릭스": "NFLX",
    "boeing": "BA",
    "보잉": "BA",
    "salesforce": "CRM",
    "세일즈포스": "CRM",
    "eli lilly": "LLY",
    "일라이릴리": "LLY",
    "일라이 릴리": "LLY",
    "jpmorgan": "JPM",
    "jp모건": "JPM",
    "제이피모건": "JPM",
    "berkshire": "BRK-B",
    "berkshire hathaway": "BRK-B",
    "버크셔": "BRK-B",
    "버크셔 해서웨이": "BRK-B",
    "버크셔해서웨이": "BRK-B",
    "disney": "DIS",
    "디즈니": "DIS",
    "visa": "V",
    "비자": "V",
    "intel": "INTC",
    "인텔": "INTC",
    "amd": "AMD",
    "에이엠디": "AMD",
    "walmart": "WMT",
    "월마트": "WMT",
    "코카콜라": "KO",
    "coca-cola": "KO",
    "pepsi": "PEP",
    "펩시": "PEP",
    "펩시코": "PEP",
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
            # 웹 검색 결과도 유효한 티커 형식인지 검증 (영문 1~5자리만 허용)
            if (
                found_ticker
                and found_ticker != "UNKNOWN"
                and found_ticker.isalpha()
                and found_ticker.isascii()
                and 1 <= len(found_ticker) <= 5
            ):
                return found_ticker.upper(), reason
    except Exception as e:
        print(f"Web search failed: {e}")

    # 실패 시: 영문(ASCII) 대문자 1~5자리라면 티커로 가정
    upper_term = term.upper()
    if upper_term.isascii() and upper_term.isalpha() and 1 <= len(upper_term) <= 5:
        return upper_term, None

    # 유효하지 않은 입력은 거부
    return None, f"'{term}'에 해당하는 기업을 찾을 수 없습니다."
