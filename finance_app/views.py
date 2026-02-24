import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.urls import reverse_lazy
from django.views import generic
from django.views.decorators.csrf import csrf_exempt

# Add project root to sys.path so we can import src
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Core Module Imports
try:
    from src.data.supabase_client import SupabaseClient, get_top_revenue_companies
    from src.tools.exchange_rate_client import get_exchange_client
    from src.core.chat_connector import ChatConnector, ChatRequest, get_chat_connector
    from src.rag.report_generator import ReportGenerator
    from src.utils.ticker_resolver import resolve_to_ticker
    from src.utils.supabase_helper import search_tickers
    from src.utils.pdf_utils import create_pdf
    from src.utils.plotly_charts import (
        generate_line_chart_plotly,
        generate_candlestick_chart_plotly,
        generate_volume_chart_plotly,
        generate_financial_chart_plotly,
    )
except ImportError as e:
    print(f"Core Import Error: {e}")
    SupabaseClient = None
    get_exchange_client = None
    get_chat_connector = None
    ReportGenerator = None
    resolve_to_ticker = None
    search_tickers = None
    create_pdf = None

from .models import Watchlist

logger = logging.getLogger(__name__)


def format_number(value, unit=""):
    """숫자 포맷팅 (app.py 에서 가져옴)"""
    if pd.isna(value) or value is None:
        return "-"
    if abs(value) >= 1e12:
        return f"${value/1e12:.1f}조{unit}"
    elif abs(value) >= 1e9:
        return f"${value/1e9:.1f}B{unit}"
    elif abs(value) >= 1e6:
        return f"${value/1e6:.1f}M{unit}"
    else:
        return f"${value:,.0f}{unit}"


def home(request):
    """홈 대시보드 뷰"""

    # 환율 정보 (기본값)
    exchange_rates = {}
    update_time = ""
    try:
        if get_exchange_client:
            client = get_exchange_client()
            rates_summary = client.get_major_rates_summary()
            if rates_summary:
                exchange_rates = rates_summary.get("display_rates", {})
                update_time = rates_summary.get("update_time", "")
    except Exception as e:
        print(f"Exchange Rate Error: {e}")

    # 데이터베이스 연동 정보 (매출 상위, 전체 기업 등)
    company_count = 0
    top_revenue_data = []  # JSON serialize 가능한 형태로 변환
    sector_counts = {}

    if SupabaseClient:
        try:
            # 전체 기업 (개수 파악 및 섹터 파악)
            companies_df = SupabaseClient.get_all_companies()
            company_count = len(companies_df)

            if "sector" in companies_df.columns:
                s_counts = companies_df["sector"].value_counts()
                for s, count in s_counts.items():
                    if (
                        s
                        and not str(s).strip().isdigit()
                        and str(s).strip() != "11"
                        and str(s).lower() != "nan"
                    ):
                        sector_counts[s] = int(count)

            # 매출 상위
            top_df = SupabaseClient.get_top_companies_by_revenue(year=2025, limit=10)
            if not top_df.empty:
                for _, row in top_df.iterrows():
                    top_revenue_data.append(
                        {
                            "ticker": row["ticker"],
                            "company_name": row["company_name"],
                            "revenue": format_number(row.get("revenue")),
                            "net_income": format_number(row.get("net_income")),
                            "total_assets": format_number(row.get("total_assets")),
                            "raw_revenue": (
                                row.get("revenue", 0) / 1e9
                                if pd.notna(row.get("revenue"))
                                else 0
                            ),  # Plotly 바 차트용 데이터
                        }
                    )
        except Exception as e:
            print(f"Supabase Data Error: {e}")

    # DB 현황용 - 등록된 기업 일부 (최대 15개)
    db_companies_sample = []
    if SupabaseClient:
        try:
            all_df = SupabaseClient.get_all_companies()
            if not all_df.empty and "ticker" in all_df.columns:
                sample = all_df.head(15)
                for _, r in sample.iterrows():
                    db_companies_sample.append(
                        {
                            "ticker": r.get("ticker", ""),
                            "company_name": r.get("company_name", ""),
                        }
                    )
        except Exception:
            pass

    context = {
        "exchange_rates": exchange_rates,
        "update_time": update_time,
        "company_count": company_count,
        "top_revenue_data": top_revenue_data,
        "top_revenue_json": json.dumps(
            [
                {"ticker": d["ticker"], "revenue": d["raw_revenue"]}
                for d in top_revenue_data
            ]
        ),
        "sector_counts_json": json.dumps(
            [{"label": k, "value": v} for k, v in sector_counts.items()]
        ),
        "db_companies_sample": db_companies_sample,
    }

    return render(request, "finance_app/home.html", context)


def search_companies_api(request):
    """기업 검색 API (GET ?q=keyword)"""
    q = request.GET.get("q", "").strip()
    if not q or len(q) < 1:
        return JsonResponse({"results": []})

    results = []
    if SupabaseClient:
        try:
            # SupabaseClient.search_companies()는 ticker, company_name, korean_name 모두 검색
            df = SupabaseClient.search_companies(q)
            if not df.empty:
                for _, r in df.head(20).iterrows():
                    results.append(
                        {
                            "ticker": r.get("ticker", ""),
                            "company_name": r.get("company_name", ""),
                            "korean_name": r.get("korean_name", ""),
                            "sector": r.get("sector", ""),
                        }
                    )
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    return JsonResponse({"results": results})


def chat(request):
    """채팅 페이지 뷰"""
    # 챗 세션 ID 초기화
    if not request.session.session_key:
        request.session.save()

    if "chat_session_id" not in request.session:
        request.session["chat_session_id"] = str(uuid.uuid4())[:16]

    return render(request, "finance_app/chat.html")


@csrf_exempt
def chat_api(request):
    """채팅 메시지 처리 API"""
    if request.method != "POST":
        return JsonResponse({"error": "POST 메서드만 지원합니다."}, status=405)

    if not get_chat_connector:
        return JsonResponse(
            {"error": "채팅 모듈(ChatConnector)을 로드할 수 없습니다."}, status=500
        )

    try:
        body = json.loads(request.body)
        message = body.get("message", "").strip()

        if not message:
            return JsonResponse({"error": "메시지가 비어 있습니다."}, status=400)

        # 세션 가져오기
        session_id = request.session.get("chat_session_id", str(uuid.uuid4())[:16])
        request.session["chat_session_id"] = session_id  # 확실히 저장

        from src.core.chat_connector import ChatRequest

        # 커넥터 가져오기 및 메시지 처리
        connector = get_chat_connector(strict_mode=False)
        chat_request = ChatRequest(session_id=session_id, message=message, use_rag=True)

        response = connector.process_message(chat_request)

        if response.success:
            return JsonResponse(
                {
                    "success": True,
                    "content": response.content,
                    "chart_data": response.chart_data,
                    "recommendations": response.recommendations,
                }
            )
        else:
            return JsonResponse(
                {
                    "success": False,
                    "content": response.content,
                    "error_code": response.error_code,
                }
            )

    except json.JSONDecodeError:
        return JsonResponse({"error": "잘못된 JSON 형식입니다."}, status=400)
    except Exception as e:
        print(f"Chat API Error: {e}")
        return JsonResponse({"error": str(e)}, status=500)


def calendar_view(request):
    """실적 캘린더 페이지 뷰"""
    return render(request, "finance_app/calendar.html")


@csrf_exempt
def calendar_api(request):
    """실적 캘린더 데이터 API"""
    if request.method != "POST":
        return JsonResponse({"error": "POST 메서드만 지원합니다."}, status=405)

    try:
        body = json.loads(request.body)
        year = int(body.get("year", datetime.now().year))
        quarter = int(body.get("quarter", (datetime.now().month - 1) // 3 + 1))

        # 관심 기업 (Watchlist) - DB 모델에서 가져오기
        default_watchlist = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]
        if request.user.is_authenticated:
            user_tickers = list(
                Watchlist.objects.filter(user=request.user).values_list(
                    "ticker", flat=True
                )
            )
            watchlist = user_tickers if user_tickers else default_watchlist
        else:
            watchlist = default_watchlist

        q_map = {
            1: ("01-01", "03-31"),
            2: ("04-01", "06-30"),
            3: ("07-01", "09-30"),
            4: ("10-01", "12-31"),
        }

        start_md, end_md = q_map.get(quarter, ("01-01", "12-31"))
        start_date = datetime.strptime(f"{year}-{start_md}", "%Y-%m-%d").date()
        end_date = datetime.strptime(f"{year}-{end_md}", "%Y-%m-%d").date()

        results = []

        import yfinance as yf

        for ticker in watchlist:
            try:
                stock = yf.Ticker(ticker)
                dates_df = stock.earnings_dates
                if dates_df is not None and not dates_df.empty:
                    for date_idx, row in dates_df.iterrows():
                        # yfinance returns timezone-aware datetimes like '2025-01-28 00:00:00-05:00'
                        if pd.isna(date_idx):
                            continue
                        e_date = date_idx.date()
                        if start_date <= e_date <= end_date:
                            eps_est = row.get("EPS Estimate")
                            eps_act = row.get("Reported EPS")
                            surprise = row.get("Surprise(%)")

                            results.append(
                                {
                                    "date": e_date.strftime("%Y-%m-%d"),
                                    "ticker": ticker,
                                    "eps_estimate": (
                                        f"{eps_est:.2f}" if pd.notna(eps_est) else "-"
                                    ),
                                    "eps_actual": (
                                        f"{eps_act:.2f}" if pd.notna(eps_act) else "-"
                                    ),
                                    "surprise": (
                                        f"{surprise:.1f}%"
                                        if pd.notna(surprise)
                                        else "-"
                                    ),
                                }
                            )
            except Exception as e:
                print(f"Error fetching calendar for {ticker}: {e}")

        # Sort results by date
        results.sort(key=lambda x: x["date"])

        return JsonResponse(
            {
                "success": True,
                "watchlist_count": len(watchlist),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "data": results,
            }
        )

    except json.JSONDecodeError:
        return JsonResponse({"error": "잘못된 JSON 형식입니다."}, status=400)
    except Exception as e:
        print(f"Calendar API Error: {e}")
        return JsonResponse({"error": str(e)}, status=500)


def report(request):
    """레포트 생성 뷰"""
    return render(request, "finance_app/report.html")


@csrf_exempt
def search_tickers_api(request):
    """티커 자동완성 검색 API"""
    query = request.GET.get("q", "")
    if not query:
        return JsonResponse({"results": []})

    if not search_tickers:
        return JsonResponse({"results": []})

    try:
        results = search_tickers(query)
        # results is a list of tuples like [('AAPL - Apple Inc.', 'AAPL'), ...]
        # We need to map it to JSON for select2 or custom dropdown
        formatted_results = [{"label": item[0], "value": item[1]} for item in results]
        return JsonResponse({"results": formatted_results[:10]})
    except Exception as e:
        print(f"Error searching tickers: {e}")
        return JsonResponse({"results": []})


@csrf_exempt
def generate_report_api(request):
    """레포트 생성 API"""
    if request.method != "POST":
        return JsonResponse({"error": "POST 메서드만 지원합니다."}, status=405)

    if not ReportGenerator or not resolve_to_ticker:
        return JsonResponse(
            {"error": "레포트 생성 모듈을 로드할 수 없습니다."}, status=500
        )

    try:
        body = json.loads(request.body)
        tickers = body.get("tickers", [])
        charts_req = body.get(
            "charts", {"line": True, "candle": False, "volume": False, "finance": False}
        )

        if not tickers:
            return JsonResponse(
                {"error": "티커를 하나 이상 제공해야 합니다."}, status=400
            )

        generator = ReportGenerator()

        # Resolve tickers to make sure we only have ticker symbols
        resolved_tickers = []
        for t in tickers:
            resolved_t, _ = resolve_to_ticker(t.strip())
            resolved_tickers.append(resolved_t)

        report_md = ""
        file_prefix = ""

        try:
            if len(resolved_tickers) > 1:
                report_md = generator.generate_comparison_report(resolved_tickers)
                file_prefix = f"comparison_{'_'.join(resolved_tickers)}"
            else:
                report_md = generator.generate_report(resolved_tickers[0])
                file_prefix = f"{resolved_tickers[0]}_analysis_report"
        except Exception as e:
            return JsonResponse(
                {"error": f"LLM 레포트 생성 중 오류: {str(e)}"}, status=500
            )

        # 차트 생성 (Plotly JSON으로 직렬화)
        charts_plotly_json = []
        try:
            for t in resolved_tickers:
                chart_data = {"ticker": t}
                if charts_req.get("line"):
                    chart_data["line_chart"] = generate_line_chart_plotly(t).to_json()
                if charts_req.get("candle"):
                    chart_data["candle_chart"] = generate_candlestick_chart_plotly(
                        t
                    ).to_json()
                if charts_req.get("volume"):
                    try:
                        chart_data["volume_chart"] = generate_volume_chart_plotly(
                            t
                        ).to_json()
                    except Exception as ve:
                        print(f"Volume chart error for {t}: {ve}")
                if charts_req.get("finance"):
                    try:
                        chart_data["finance_chart"] = generate_financial_chart_plotly(
                            t
                        ).to_json()
                    except Exception as fe:
                        print(f"Finance chart error for {t}: {fe}")

                charts_plotly_json.append(chart_data)
        except Exception as e:
            print(f"차트 생성 중 오류: {e}")

        return JsonResponse(
            {
                "success": True,
                "report_md": report_md,
                "file_prefix": file_prefix,
                "charts": charts_plotly_json,
            }
        )

    except Exception as e:
        print(f"Generate report error: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
def download_report_pdf(request):
    """PDF 다운로드 API"""
    if request.method != "POST":
        return HttpResponse("POST 메서드만 지원합니다.", status=405)

    if not create_pdf:
        return HttpResponse("PDF 생성 모듈을 로드할 수 없습니다.", status=500)

    try:
        report_md = request.POST.get("report_md", "")
        file_prefix = request.POST.get("file_prefix", "report")

        # 차트 이미지는 현재 생략하고 마크다운 텍스트만 PDF로 변환
        pdf_bytes = create_pdf(report_md, chart_images=[])

        response = HttpResponse(pdf_bytes, content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{file_prefix}.pdf"'
        return response
    except Exception as e:
        print(f"PDF 생성 오류: {e}")
        return HttpResponse(f"PDF 생성 실패: {str(e)}", status=500)


class SignUpView(generic.CreateView):
    form_class = UserCreationForm
    success_url = reverse_lazy("login")
    template_name = "finance_app/signup.html"


# ─── Watchlist API ────────────────────────────────────────────────────


@login_required
def watchlist_add(request):
    """관심 기업 추가 API (POST)"""
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)
    try:
        data = json.loads(request.body)
        raw_input = data.get("ticker", "").strip()
        if not raw_input:
            return JsonResponse({"error": "ticker required"}, status=400)

        # 한글명/영문명 → 티커 자동 변환 (예: "애플" → "AAPL")
        if resolve_to_ticker:
            ticker, reason = resolve_to_ticker(raw_input)
        else:
            ticker = raw_input.upper()
            reason = None

        obj, created = Watchlist.objects.get_or_create(user=request.user, ticker=ticker)
        return JsonResponse(
            {
                "success": True,
                "created": created,
                "ticker": ticker,
                "resolved_from": raw_input if raw_input.upper() != ticker else None,
            }
        )
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def watchlist_remove(request):
    """관심 기업 삭제 API (POST)"""
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)
    try:
        data = json.loads(request.body)
        ticker = data.get("ticker", "").strip().upper()
        deleted, _ = Watchlist.objects.filter(user=request.user, ticker=ticker).delete()
        return JsonResponse({"success": True, "deleted": deleted > 0, "ticker": ticker})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def watchlist_list(request):
    """관심 기업 목록 API (GET)"""
    tickers = list(
        Watchlist.objects.filter(user=request.user).values_list("ticker", flat=True)
    )
    return JsonResponse({"tickers": tickers})
