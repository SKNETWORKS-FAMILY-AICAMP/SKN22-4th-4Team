import json
import logging
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required

logger = logging.getLogger(__name__)

# Core imports
try:
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
    logger.error(f"Core Import Error in report_views: {e}")
    ReportGenerator = None
    resolve_to_ticker = None
    search_tickers = None
    create_pdf = None

# Celery 가용 여부 확인
try:
    from celery.result import AsyncResult
    from finance_app.tasks import orchestrate_report_pipeline
    CELERY_AVAILABLE = True
    logger.info("Celery 사용 가능: 비동기 레포트 생성 모드 활성화")
except ImportError:
    CELERY_AVAILABLE = False
    logger.warning("Celery 불가: 동기 모드로 폴백")


@login_required
def report_view(request):
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
        formatted_results = [{"label": item[0], "value": item[1]} for item in results]
        return JsonResponse({"results": formatted_results[:10]})
    except Exception as e:
        logger.error(f"Error searching tickers: {e}")
        return JsonResponse({"results": []})


# ─────────────────────────────────────────────────────────────────────────────
# [비동기] 레포트 생성 작업 제출 API
# ─────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@login_required
def generate_report_api(request):
    """
    레포트 생성 API.
    - Celery 사용 가능: 작업을 큐에 제출하고 task_id 즉시 반환 (비동기)
    - Celery 불가: 기존 동기 방식으로 폴백
    """
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

        # 티커 해석 (한글 회사명 → 티커 변환)
        resolved_tickers = []
        for t in tickers:
            resolved_t, _ = resolve_to_ticker(t.strip())
            resolved_tickers.append(resolved_t)

        # ── 비동기 모드 (Celery 사용 가능) ──────────────────────────────────
        if CELERY_AVAILABLE:
            task = orchestrate_report_pipeline.apply_async(
                args=[resolved_tickers, charts_req],
                queue="heavy",
            )
            logger.info(f"[ASYNC] 레포트 파이프라인 태스크 제출: task_id={task.id}, tickers={resolved_tickers}")
            return JsonResponse({
                "async": True,
                "task_id": task.id,
                "message": f"레포트 생성 작업이 시작되었습니다. (Task ID: {task.id})",
            })

        # ── 동기 폴백 모드 (Celery 없을 때) ─────────────────────────────────
        logger.info("[SYNC] Celery 없이 동기 방식으로 레포트 생성")
        return _generate_report_sync(resolved_tickers, charts_req)

    except Exception as e:
        logger.error(f"Generate report error: {e}")
        return JsonResponse({"error": str(e)}, status=500)


def _generate_report_sync(resolved_tickers: list, charts_req: dict) -> JsonResponse:
    """동기 방식 레포트 생성 (Celery 폴백용)"""
    generator = ReportGenerator()
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

    charts_plotly_json = []
    try:
        for t in resolved_tickers:
            chart_data = {"ticker": t}
            if charts_req.get("line"):
                chart_data["line_chart"] = generate_line_chart_plotly([t]).to_json()
            if charts_req.get("candle"):
                chart_data["candle_chart"] = generate_candlestick_chart_plotly([t]).to_json()
            if charts_req.get("volume"):
                try:
                    chart_data["volume_chart"] = generate_volume_chart_plotly([t]).to_json()
                except Exception as ve:
                    logger.error(f"Volume chart error for {t}: {ve}")
            if charts_req.get("finance"):
                try:
                    chart_data["finance_chart"] = generate_financial_chart_plotly([t]).to_json()
                except Exception as fe:
                    logger.error(f"Finance chart error for {t}: {fe}")
            charts_plotly_json.append(chart_data)
    except Exception as e:
        logger.error(f"차트 생성 중 오류: {e}")

    return JsonResponse({
        "success": True,
        "async": False,
        "report_md": report_md,
        "file_prefix": file_prefix,
        "charts": charts_plotly_json,
    })


# ─────────────────────────────────────────────────────────────────────────────
# [비동기] 태스크 상태 폴링 API
# ─────────────────────────────────────────────────────────────────────────────

@login_required
def report_task_status(request, task_id: str):
    """
    Celery 태스크 상태 폴링 API.
    프론트엔드에서 일정 간격으로 호출하여 작업 완료 여부 확인.

    Returns:
        state: PENDING | STARTED | SUCCESS | FAILURE | RETRY
        result: 완료 시 레포트 데이터 (state == SUCCESS인 경우)
        error: 실패 시 오류 메시지
    """
    if not CELERY_AVAILABLE:
        return JsonResponse({"error": "Celery가 활성화되어 있지 않습니다."}, status=400)

    try:
        task_result = AsyncResult(task_id)
        state = task_result.state

        if state == "PENDING":
            return JsonResponse({"state": "PENDING", "message": "작업 대기 중..."})

        elif state == "STARTED":
            return JsonResponse({"state": "STARTED", "message": "레포트 생성 중..."})

        elif state == "RETRY":
            return JsonResponse({"state": "RETRY", "message": "재시도 중..."})

        elif state == "SUCCESS":
            result = task_result.result
            if result and result.get("success"):
                logger.info(f"[POLL] 태스크 완료: task_id={task_id}")
                return JsonResponse({
                    "state": "SUCCESS",
                    "success": True,
                    "report_md": result.get("report_md", ""),
                    "file_prefix": result.get("file_prefix", "report"),
                    "charts": result.get("charts", []),
                })
            else:
                return JsonResponse({
                    "state": "FAILURE",
                    "error": result.get("error", "알 수 없는 오류가 발생했습니다."),
                })

        elif state == "FAILURE":
            error_info = str(task_result.result) if task_result.result else "알 수 없는 오류"
            logger.error(f"[POLL] 태스크 실패: task_id={task_id}, error={error_info}")
            return JsonResponse({
                "state": "FAILURE",
                "error": f"레포트 생성 실패: {error_info}",
            })

        else:
            return JsonResponse({"state": state, "message": f"처리 중... ({state})"})

    except Exception as e:
        logger.error(f"Task status check error: task_id={task_id}, error={e}")
        return JsonResponse({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# PDF 다운로드 API (동기 유지 - PDF 자체는 빠름)
# ─────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@login_required
def download_report_pdf(request):
    """PDF 다운로드 API"""
    if request.method != "POST":
        return HttpResponse("POST 메서드만 지원합니다.", status=405)

    if not create_pdf:
        return HttpResponse("PDF 생성 모듈을 로드할 수 없습니다.", status=500)

    try:
        report_md = request.POST.get("report_md", "")
        file_prefix = request.POST.get("file_prefix", "report")

        pdf_bytes = create_pdf(report_md, chart_images=[])

        response = HttpResponse(pdf_bytes, content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{file_prefix}.pdf"'
        return response
    except Exception as e:
        logger.error(f"PDF 생성 오류: {e}")
        return HttpResponse(f"PDF 생성 실패: {str(e)}", status=500)
