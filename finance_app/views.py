import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm, PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
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


from django.core.cache import cache


def home(request):
    """홈 대시보드 뷰 (캐싱 + 병렬 처리 최적화)"""
    from concurrent.futures import ThreadPoolExecutor

    # 1. 캐시 시도 (10분 유효)
    cache_key = "home_dashboard_context"
    cached_context = cache.get(cache_key)

    if cached_context:
        return render(request, "finance_app/home.html", cached_context)

    # 2. 캐시 미스: 병렬로 데이터 수집
    results = {
        "exchange_rates": {},
        "update_time": "",
        "company_count": 0,
        "top_revenue_data": [],
        "sector_counts": {},
        "db_companies_sample": [],
    }

    def fetch_exchange_rates():
        try:
            if get_exchange_client:
                client = get_exchange_client()
                rates_summary = client.get_major_rates_summary()
                if rates_summary:
                    results["exchange_rates"] = rates_summary.get("display_rates", {})
                    results["update_time"] = rates_summary.get("update_time", "")
        except Exception as e:
            logger.warning(f"Exchange Rate Error: {e}")

    def fetch_company_data():
        if not SupabaseClient:
            return
        try:
            # 전체 기업 (1회만 조회 — 기존 2회 호출 제거)
            companies_df = SupabaseClient.get_all_companies()
            results["company_count"] = len(companies_df)

            # 섹터별 분류
            if "sector" in companies_df.columns:
                s_counts = companies_df["sector"].value_counts()
                for s, count in s_counts.items():
                    if (
                        s
                        and not str(s).strip().isdigit()
                        and str(s).strip() != "11"
                        and str(s).lower() != "nan"
                    ):
                        results["sector_counts"][s] = int(count)

            # DB 현황 샘플 (기존 데이터 재사용)
            if not companies_df.empty and "ticker" in companies_df.columns:
                sample = companies_df.head(15)
                for _, r in sample.iterrows():
                    results["db_companies_sample"].append(
                        {
                            "ticker": r.get("ticker", ""),
                            "company_name": r.get("company_name", ""),
                        }
                    )
        except Exception as e:
            logger.warning(f"Supabase Companies Error: {e}")

        # 매출 상위
        try:
            top_df = SupabaseClient.get_top_companies_by_revenue(year=2025, limit=10)
            if not top_df.empty:
                for _, row in top_df.iterrows():
                    results["top_revenue_data"].append(
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
                            ),
                        }
                    )
        except Exception as e:
            logger.warning(f"Supabase Revenue Error: {e}")

    # 환율 + DB 쿼리 동시 실행
    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(fetch_exchange_rates)
        f2 = executor.submit(fetch_company_data)
        f1.result()
        f2.result()

    # 컨텍스트 조립
    context = {
        "exchange_rates": results["exchange_rates"],
        "update_time": results["update_time"],
        "company_count": results["company_count"],
        "top_revenue_data": results["top_revenue_data"],
        "top_revenue_json": json.dumps(
            [
                {"ticker": d["ticker"], "revenue": d["raw_revenue"]}
                for d in results["top_revenue_data"]
            ]
        ),
        "sector_counts_json": json.dumps(
            [{"label": k, "value": v} for k, v in results["sector_counts"].items()]
        ),
        "db_companies_sample": results["db_companies_sample"],
    }

    # 3. 데이터 캐싱 (600초 = 10분)
    cache.set(cache_key, context, 600)

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


@login_required
def chat(request):
    """채팅 페이지 뷰"""
    # 챗 세션 ID 초기화
    if not request.session.session_key:
        request.session.save()

    if "chat_session_id" not in request.session:
        request.session["chat_session_id"] = str(uuid.uuid4())[:16]

    return render(request, "finance_app/chat.html")


@csrf_exempt
@login_required
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
        chat_request = ChatRequest(
            session_id=session_id,
            message=message,
            ticker=None,
            use_rag=True,
        )

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


@login_required
def calendar_view(request):
    """실적 캘린더 페이지 뷰"""
    return render(request, "finance_app/calendar.html")


@csrf_exempt
@login_required
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


@login_required
def profile_view(request):
    """프로필 설정 페이지"""
    # 소셜 로그인 가입자 접근 차단 로직 (보안 레이어 추가)
    if not request.user.has_usable_password():
        messages.error(request, "소셜 계정은 해당 페이지에 접근할 수 없습니다.")
        return redirect("home")

    try:
        from .forms import ProfileForm
    except ImportError:
        pass

    profile_form = (
        ProfileForm(instance=request.user) if "ProfileForm" in locals() else None
    )
    password_form = PasswordChangeForm(request.user)
    if "old_password" in password_form.fields:
        password_form.fields["old_password"].widget.attrs.pop("autofocus", None)

    if request.method == "POST":
        is_ajax = request.headers.get("x-requested-with") == "XMLHttpRequest"

        if "update_profile" in request.POST and profile_form:
            profile_form = ProfileForm(request.POST, instance=request.user)
            if profile_form.is_valid():
                profile_form.save()
                if is_ajax:
                    return JsonResponse(
                        {
                            "success": True,
                            "message": "닉네임이 성공적으로 변경되었습니다.",
                        }
                    )
                messages.success(request, "닉네임이 성공적으로 변경되었습니다.")
                return redirect("finance_app:profile")
            elif is_ajax:
                return JsonResponse({"success": False, "errors": profile_form.errors})

        elif "update_password" in request.POST:
            password_form = PasswordChangeForm(request.user, request.POST)
            if "old_password" in password_form.fields:
                password_form.fields["old_password"].widget.attrs.pop("autofocus", None)

            if password_form.is_valid():
                user = password_form.save()
                update_session_auth_hash(
                    request, user
                )  # 비밀번호 변경 후 로그인 강제해제 방지
                if is_ajax:
                    return JsonResponse(
                        {
                            "success": True,
                            "message": "비밀번호가 성공적으로 변경되었습니다.",
                        }
                    )
                messages.success(request, "비밀번호가 성공적으로 변경되었습니다.")
                return redirect("finance_app:profile")
            elif is_ajax:
                return JsonResponse({"success": False, "errors": password_form.errors})

    return render(
        request,
        "finance_app/profile.html",
        {
            "profile_form": profile_form,
            "password_form": password_form,
            "is_social": not request.user.has_usable_password(),
        },
    )
