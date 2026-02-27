"""
Celery 비동기 태스크 정의
- 레포트 생성 (RAG + LLM) 태스크
- 차트 생성 태스크
- 비교 레포트 생성 태스크
"""

import logging
from celery import shared_task, group, chord
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 단일 레포트 생성 태스크
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    name="finance_app.tasks.generate_report_task",
    max_retries=2,
    soft_time_limit=300,   # 5분 소프트 타임아웃
    time_limit=360,        # 6분 하드 타임아웃
)
def generate_report_task(self, ticker: str) -> dict:
    """
    단일 종목 투자 분석 레포트 생성 비동기 태스크.
    Celery Worker에서 실행되므로 블로킹 I/O (LLM 호출, DB 조회) 가능.
    """
    logger.info(f"[TASK] generate_report_task 시작: ticker={ticker}")
    try:
        from src.rag.report_generator import ReportGenerator

        generator = ReportGenerator()
        report_md = generator.generate_report(ticker)
        file_prefix = f"{ticker}_analysis_report"

        logger.info(f"[TASK] generate_report_task 완료: ticker={ticker}")
        return {"success": True, "report_md": report_md, "file_prefix": file_prefix}

    except Exception as exc:
        logger.error(f"[TASK] generate_report_task 실패: ticker={ticker}, error={exc}")
        # 재시도 (exponential backoff)
        raise self.retry(exc=exc, countdown=10 * (self.request.retries + 1))


# ─────────────────────────────────────────────────────────────────────────────
# 비교 레포트 생성 태스크
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    name="finance_app.tasks.generate_comparison_report_task",
    max_retries=2,
    soft_time_limit=480,
    time_limit=540,
)
def generate_comparison_report_task(self, tickers: list) -> dict:
    """
    복수 종목 비교 레포트 생성 비동기 태스크.
    내부적으로 각 종목 데이터를 병렬 fetch 후 LLM 호출.
    """
    logger.info(f"[TASK] generate_comparison_report_task 시작: tickers={tickers}")
    try:
        from src.rag.report_generator import ReportGenerator

        generator = ReportGenerator()
        report_md = generator.generate_comparison_report(tickers)
        file_prefix = f"comparison_{'_'.join(tickers)}"

        logger.info(f"[TASK] generate_comparison_report_task 완료")
        return {"success": True, "report_md": report_md, "file_prefix": file_prefix}

    except Exception as exc:
        logger.error(f"[TASK] generate_comparison_report_task 실패: {exc}")
        raise self.retry(exc=exc, countdown=15 * (self.request.retries + 1))


# ─────────────────────────────────────────────────────────────────────────────
# 차트 생성 태스크 (티커 하나에 대해 모든 차트 병렬 생성용 서브태스크)
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    name="finance_app.tasks.generate_charts_for_ticker_task",
    soft_time_limit=120,
    time_limit=150,
)
def generate_charts_for_ticker_task(ticker: str, charts_req: dict) -> dict:
    """
    단일 티커에 대한 Plotly 차트 JSON 생성 태스크.
    여러 티커를 병렬로 처리할 때 Celery group으로 묶어서 사용.
    """
    logger.info(f"[TASK] generate_charts_for_ticker_task 시작: ticker={ticker}")
    try:
        from src.utils.plotly_charts import (
            generate_line_chart_plotly,
            generate_candlestick_chart_plotly,
            generate_volume_chart_plotly,
            generate_financial_chart_plotly,
        )

        chart_data = {"ticker": ticker}

        if charts_req.get("line"):
            chart_data["line_chart"] = generate_line_chart_plotly(ticker).to_json()

        if charts_req.get("candle"):
            chart_data["candle_chart"] = generate_candlestick_chart_plotly(ticker).to_json()

        if charts_req.get("volume"):
            try:
                chart_data["volume_chart"] = generate_volume_chart_plotly(ticker).to_json()
            except Exception as ve:
                logger.warning(f"Volume chart 생성 실패 ({ticker}): {ve}")

        if charts_req.get("finance"):
            try:
                chart_data["finance_chart"] = generate_financial_chart_plotly(ticker).to_json()
            except Exception as fe:
                logger.warning(f"Finance chart 생성 실패 ({ticker}): {fe}")

        logger.info(f"[TASK] generate_charts_for_ticker_task 완료: ticker={ticker}")
        return chart_data

    except Exception as exc:
        logger.error(f"[TASK] generate_charts_for_ticker_task 실패: ticker={ticker}, error={exc}")
        # 차트는 실패해도 빈 dict 반환 (레포트 자체에는 영향 없음)
        return {"ticker": ticker}


# ─────────────────────────────────────────────────────────────────────────────
# 레포트 + 차트 전체 파이프라인 오케스트레이터 태스크
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    name="finance_app.tasks.orchestrate_report_pipeline",
    soft_time_limit=600,
    time_limit=660,
)
def orchestrate_report_pipeline(self, tickers: list, charts_req: dict) -> dict:
    """
    레포트 생성 + 차트 생성을 병렬로 실행하여 합산 결과를 반환하는 오케스트레이터.

    병렬 처리 전략:
    - 레포트 생성 태스크 (LLM 호출) ──┐
    - 차트 생성 태스크들 (병렬 그룹) ──┴──→ 결과 합산 → 반환

    Celery group을 통해 레포트와 차트를 동시에 실행.
    """
    logger.info(f"[ORCHESTRATOR] 파이프라인 시작: tickers={tickers}")

    try:
        import concurrent.futures

        # ── 1. 레포트 생성 + 차트 생성을 Python ThreadPoolExecutor로 병렬 실행
        #    (Celery chord/group을 사용하면 더 강건하지만, 단순함을 위해 ThreadPool 사용)
        from src.rag.report_generator import ReportGenerator
        from src.utils.plotly_charts import (
            generate_line_chart_plotly,
            generate_candlestick_chart_plotly,
            generate_volume_chart_plotly,
            generate_financial_chart_plotly,
        )

        report_md = ""
        file_prefix = ""
        charts_plotly_json = []

        def _generate_report():
            generator = ReportGenerator()
            if len(tickers) > 1:
                md = generator.generate_comparison_report(tickers)
                prefix = f"comparison_{'_'.join(tickers)}"
            else:
                md = generator.generate_report(tickers[0])
                prefix = f"{tickers[0]}_analysis_report"
            return md, prefix

        def _generate_chart_for_ticker(ticker):
            chart_data = {"ticker": ticker}
            try:
                if charts_req.get("line"):
                    chart_data["line_chart"] = generate_line_chart_plotly(ticker).to_json()
                if charts_req.get("candle"):
                    chart_data["candle_chart"] = generate_candlestick_chart_plotly(ticker).to_json()
                if charts_req.get("volume"):
                    try:
                        chart_data["volume_chart"] = generate_volume_chart_plotly(ticker).to_json()
                    except Exception:
                        pass
                if charts_req.get("finance"):
                    try:
                        chart_data["finance_chart"] = generate_financial_chart_plotly(ticker).to_json()
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"차트 생성 실패 ({ticker}): {e}")
            return chart_data

        # ── 2. ThreadPoolExecutor로 레포트 + 차트 병렬 실행
        futures_results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tickers) + 1) as executor:
            # 레포트 생성 Future
            report_future = executor.submit(_generate_report)
            # 차트 생성 Futures (티커별)
            chart_futures = {
                executor.submit(_generate_chart_for_ticker, ticker): ticker
                for ticker in tickers
            }

            # 레포트 결과 수집
            report_md, file_prefix = report_future.result(timeout=480)
            logger.info(f"[ORCHESTRATOR] 레포트 생성 완료: {file_prefix}")

            # 차트 결과 수집
            for future, ticker in chart_futures.items():
                try:
                    chart_data = future.result(timeout=120)
                    charts_plotly_json.append(chart_data)
                    logger.info(f"[ORCHESTRATOR] 차트 생성 완료: {ticker}")
                except Exception as e:
                    logger.warning(f"[ORCHESTRATOR] 차트 생성 실패 ({ticker}): {e}")
                    charts_plotly_json.append({"ticker": ticker})

        logger.info(f"[ORCHESTRATOR] 파이프라인 완료")
        return {
            "success": True,
            "report_md": report_md,
            "file_prefix": file_prefix,
            "charts": charts_plotly_json,
        }

    except Exception as exc:
        logger.error(f"[ORCHESTRATOR] 파이프라인 실패: {exc}")
        raise self.retry(exc=exc, countdown=5)
