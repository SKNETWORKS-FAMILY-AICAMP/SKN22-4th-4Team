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
    from src.utils.ticker_search_agent import search_tickers
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


@csrf_exempt
@login_required
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
                        logger.error(f"Volume chart error for {t}: {ve}")
                if charts_req.get("finance"):
                    try:
                        chart_data["finance_chart"] = generate_financial_chart_plotly(
                            t
                        ).to_json()
                    except Exception as fe:
                        logger.error(f"Finance chart error for {t}: {fe}")

                charts_plotly_json.append(chart_data)
        except Exception as e:
            logger.error(f"차트 생성 중 오류: {e}")

        return JsonResponse(
            {
                "success": True,
                "report_md": report_md,
                "file_prefix": file_prefix,
                "charts": charts_plotly_json,
            }
        )

    except Exception as e:
        logger.error(f"Generate report error: {e}")
        return JsonResponse({"error": str(e)}, status=500)


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
