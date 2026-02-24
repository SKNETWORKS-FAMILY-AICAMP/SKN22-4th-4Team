"""
Plotly Chart Utilities - 선명한 벡터 기반 차트 생성 모듈
- Streamlit에서 항상 선명한 인터랙티브 차트
- PDF 내보내기용 고해상도 이미지 생성 (kaleido)
- 한글 폰트 자동 지원
"""

import logging
from io import BytesIO
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from functools import lru_cache

logger = logging.getLogger(__name__)

# 색상 팔레트
COLORS = ["#2196f3", "#4caf50", "#ff9800", "#e91e63", "#9c27b0", "#00bcd4"]
UP_COLOR = "#26a69a"  # 상승 - 초록
DOWN_COLOR = "#ef5350"  # 하락 - 빨강


# ============================================================
# DATA FETCHING LAYER (캐싱 적용)
# ============================================================


@lru_cache(maxsize=50)
def _fetch_stock_history(ticker: str, days: int) -> Optional[Tuple]:
    """주가 데이터 캐싱"""
    try:
        import yfinance as yf

        end_d = datetime.now()
        start_d = end_d - timedelta(days=days)
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_d, end=end_d)
        if df.empty:
            return None
        return (
            tuple(df.index.tolist()),
            tuple(df["Open"].tolist()),
            tuple(df["High"].tolist()),
            tuple(df["Low"].tolist()),
            tuple(df["Close"].tolist()),
            tuple(df["Volume"].tolist()),
        )
    except Exception as e:
        logger.warning(f"Stock data fetch failed for {ticker}: {e}")
        return None


@lru_cache(maxsize=20)
def _fetch_quarterly_financials(ticker: str) -> Optional[Tuple]:
    """분기별 재무 데이터 캐싱"""
    try:
        import yfinance as yf

        stock = yf.Ticker(ticker)
        quarterly = stock.quarterly_financials
        if quarterly.empty:
            return None

        revenue_row = net_income_row = None
        for idx in quarterly.index:
            idx_lower = str(idx).lower()
            if "revenue" in idx_lower or "total revenue" in idx_lower:
                revenue_row = idx
            if "net income" in idx_lower:
                net_income_row = idx

        if revenue_row is None:
            return None

        quarters = quarterly.columns[:8][::-1]
        revenue = quarterly.loc[revenue_row, quarters].values / 1e9
        net_income = (
            quarterly.loc[net_income_row, quarters].values / 1e9
            if net_income_row
            else None
        )
        quarter_labels = tuple(
            q.strftime("%Y Q").replace("Q", f"Q{(q.month-1)//3+1}") for q in quarters
        )
        return (
            quarter_labels,
            tuple(revenue),
            tuple(net_income) if net_income is not None else None,
        )
    except Exception as e:
        logger.warning(f"Financial data fetch failed for {ticker}: {e}")
        return None


def clear_cache():
    """모든 캐시 초기화"""
    _fetch_stock_history.cache_clear()
    _fetch_quarterly_financials.cache_clear()


# ============================================================
# PLOTLY CHART RENDERING (벡터 기반, 항상 선명)
# ============================================================


def generate_line_chart_plotly(tickers: List[str], days: int = 90):
    """주가 추이 선 그래프 (Plotly 버전)"""
    try:
        import plotly.graph_objects as go

        fig = go.Figure()
        has_data = False

        for i, ticker in enumerate(tickers):
            data = _fetch_stock_history(ticker, days)
            if data:
                dates, _, _, _, closes, _ = data
                color = COLORS[i % len(COLORS)]
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=closes,
                        mode="lines",
                        name=ticker,
                        line=dict(color=color, width=2),
                    )
                )
                has_data = True

        if not has_data:
            return None

        title = (
            f"주가 추이 ({', '.join(tickers)})"
            if len(tickers) > 1
            else f"{tickers[0]} 주가 추이"
        )

        fig.update_layout(
            title=dict(text=title, font=dict(size=18, family="Malgun Gothic")),
            xaxis_title="날짜",
            yaxis_title="주가 (USD)",
            template="plotly_white",
            hovermode="x unified",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            margin=dict(l=60, r=40, t=80, b=60),
            height=450,
        )
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.2)")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.2)")

        return fig

    except Exception as e:
        logger.warning(f"Plotly line chart failed: {e}")
        return None


def generate_candlestick_chart_plotly(tickers: List[str], days: int = 60):
    """캔들스틱 차트 (Plotly 버전)"""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        n_tickers = len(tickers)
        fig = make_subplots(
            rows=n_tickers,
            cols=1,
            subplot_titles=[f"{t} 캔들스틱 ({days}일)" for t in tickers],
            vertical_spacing=0.08,
        )

        has_any_data = False

        for idx, ticker in enumerate(tickers):
            data = _fetch_stock_history(ticker, days)
            if not data:
                continue

            has_any_data = True
            dates, opens, highs, lows, closes, _ = data

            fig.add_trace(
                go.Candlestick(
                    x=dates,
                    open=opens,
                    high=highs,
                    low=lows,
                    close=closes,
                    name=ticker,
                    increasing_line_color=UP_COLOR,
                    decreasing_line_color=DOWN_COLOR,
                ),
                row=idx + 1,
                col=1,
            )

        if not has_any_data:
            return None

        fig.update_layout(
            template="plotly_white",
            showlegend=False,
            height=350 * n_tickers,
            margin=dict(l=60, r=40, t=60, b=40),
        )
        fig.update_xaxes(rangeslider_visible=False)

        return fig

    except Exception as e:
        logger.warning(f"Plotly candlestick chart failed: {e}")
        return None


def generate_volume_chart_plotly(tickers: List[str], days: int = 60):
    """거래량 차트 (Plotly 버전)"""
    try:
        import plotly.graph_objects as go

        fig = go.Figure()
        has_data = False

        for i, ticker in enumerate(tickers):
            data = _fetch_stock_history(ticker, days)
            if not data:
                continue

            has_data = True
            dates, _, _, _, _, volumes = data
            color = COLORS[i % len(COLORS)]

            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=[v / 1e6 for v in volumes],
                    mode="lines",
                    name=ticker,
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    opacity=0.7,
                )
            )

        if not has_data:
            return None

        title = (
            f"거래량 비교 ({', '.join(tickers)})"
            if len(tickers) > 1
            else f"{tickers[0]} 거래량"
        )

        fig.update_layout(
            title=dict(text=title, font=dict(size=18, family="Malgun Gothic")),
            xaxis_title="날짜",
            yaxis_title="거래량 (백만)",
            template="plotly_white",
            hovermode="x unified",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            margin=dict(l=60, r=40, t=80, b=60),
            height=400,
        )

        return fig

    except Exception as e:
        logger.warning(f"Plotly volume chart failed: {e}")
        return None


def generate_financial_chart_plotly(tickers: List[str]):
    """분기별 재무 차트 (Plotly 버전)"""
    try:
        import plotly.graph_objects as go

        all_data = {}
        for ticker in tickers:
            data = _fetch_quarterly_financials(ticker)
            if data:
                all_data[ticker] = data

        if not all_data:
            return None

        # 공통 분기 수 결정
        min_quarters = min(len(data[0]) for data in all_data.values())
        first_ticker = list(all_data.keys())[0]
        quarter_labels = all_data[first_ticker][0][:min_quarters]

        fig = go.Figure()

        for i, (ticker, (_, revenue, _)) in enumerate(all_data.items()):
            revenue_trimmed = revenue[:min_quarters]
            color = COLORS[i % len(COLORS)]

            fig.add_trace(
                go.Bar(
                    x=quarter_labels,
                    y=revenue_trimmed,
                    name=ticker,
                    marker_color=color,
                    opacity=0.85,
                )
            )

        n_tickers = len(all_data)
        title = (
            f"분기별 매출 비교 ({', '.join(tickers)})"
            if n_tickers > 1
            else f"{first_ticker} 분기별 매출"
        )

        fig.update_layout(
            title=dict(text=title, font=dict(size=18, family="Malgun Gothic")),
            xaxis_title="분기",
            yaxis_title="매출 (십억 USD)",
            barmode="group",
            template="plotly_white",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            margin=dict(l=60, r=40, t=80, b=60),
            height=450,
        )

        return fig

    except Exception as e:
        logger.warning(f"Plotly financial chart failed: {e}")
        return None


# ============================================================
# PDF EXPORT (kaleido 사용)
# ============================================================


def plotly_to_image(fig, width: int = 1200, height: int = 600) -> Optional[BytesIO]:
    """Plotly 차트를 고해상도 PNG로 변환 (PDF용)"""
    try:
        img_bytes = fig.to_image(format="png", width=width, height=height, scale=2)
        buf = BytesIO(img_bytes)
        buf.seek(0)
        return buf
    except Exception as e:
        logger.warning(f"Plotly to image failed: {e}")
        return None


# ============================================================
# UTILITY FUNCTIONS
# ============================================================


def detect_chart_type(user_input: str) -> str:
    """사용자 입력에서 차트 타입 감지"""
    text = user_input.lower()
    if any(kw in text for kw in ["캔들", "캔들스틱", "candlestick", "candle"]):
        return "candlestick"
    if any(kw in text for kw in ["거래량", "볼륨", "volume", "매매량"]):
        return "volume"
    if any(
        kw in text
        for kw in ["매출", "순이익", "재무", "revenue", "income", "financial", "실적"]
    ):
        return "financial"
    return "line"
