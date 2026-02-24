"""
스케줄러 관리 모듈
S&P 500 데이터 수집 스케줄러 초기화 및 상태 관리
"""

import sys
import logging
from pathlib import Path
from typing import Optional, Callable

logger = logging.getLogger(__name__)

# 스케줄러 인스턴스 (모듈 레벨)
_scheduler = None
_collect_fn: Optional[Callable] = None


def init_scheduler():
    """
    백그라운드 스케줄러 초기화 (매일 05:00 KST S&P 500 데이터 수집)
    Returns: (scheduler, collect_function) 튜플 또는 (None, None)
    """
    global _scheduler, _collect_fn

    if _scheduler is not None:
        return _scheduler, _collect_fn

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz

        # 스케줄러 생성
        kst = pytz.timezone("Asia/Seoul")
        scheduler = BackgroundScheduler(timezone=kst)

        # S&P 500 데이터 수집 함수 import
        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        if not scripts_path.exists():
            logger.warning(f"스케줄러 scripts 경로 없음: {scripts_path}")
            logger.warning(
                "스케줄러가 비활성 상태로 시작됩니다 (데이터 수집 없이 대기)"
            )
            # 스케줄러는 시작하되, 수집 함수 없이 대기 상태로 둠
            scheduler.start()
            _scheduler = scheduler
            return scheduler, None

        sys.path.insert(0, str(scripts_path))
        from sp500_scheduler import collect_sp500_data

        # 매일 새벽 5시(KST) 실행
        scheduler.add_job(
            collect_sp500_data,
            CronTrigger(hour=5, minute=0, timezone=kst),
            id="sp500_daily_collection",
            name="S&P 500 Daily Data Collection",
            replace_existing=True,
        )

        scheduler.start()
        _scheduler = scheduler
        _collect_fn = collect_sp500_data

        logger.info("📅 S&P 500 스케줄러 시작됨 (매일 05:00 KST)")
        return scheduler, collect_sp500_data

    except ImportError as e:
        logger.warning(f"스케줄러 초기화 실패 (패키지 없음): {e}")
        return None, None
    except Exception as e:
        logger.error(f"스케줄러 초기화 오류: {e}")
        return None, None


def get_scheduler():
    """현재 스케줄러 인스턴스 반환"""
    return _scheduler


def get_collect_function():
    """수집 함수 반환"""
    return _collect_fn


def get_next_run_time() -> Optional[str]:
    """다음 실행 시간 문자열 반환"""
    if _scheduler is None:
        return None

    job = _scheduler.get_job("sp500_daily_collection")
    if job:
        next_run = getattr(job, "next_run_time", None)
        if next_run:
            return next_run.strftime("%Y-%m-%d %H:%M:%S %Z")
    return "매일 05:00 KST"


def is_running() -> bool:
    """스케줄러 실행 중 여부"""
    return _scheduler is not None and _scheduler.running


def run_now():
    """즉시 수집 실행"""
    if _collect_fn:
        _collect_fn()
        return True
    return False


def render_sidebar_status():
    """
    사이드바에 스케줄러 상태 UI 렌더링
    Streamlit 컨텍스트에서 호출해야 함
    """
    import streamlit as st

    with st.sidebar.expander("📅 스케줄러 상태", expanded=False):
        if is_running():
            st.success("✅ 스케줄러 실행 중")

            next_time = get_next_run_time()
            if next_time:
                st.info(f"⏰ 다음 실행: {next_time}")

            # 수동 실행 버튼
            if st.button("🔄 지금 수집 실행", key="run_scheduler_now"):
                with st.spinner("S&P 500 데이터 수집 중..."):
                    try:
                        run_now()
                        st.success("✅ 수집 완료!")
                    except Exception as e:
                        st.error(f"❌ 오류: {e}")
        else:
            st.warning("⚠️ 스케줄러 비활성")
            st.caption("APScheduler 패키지가 필요합니다.")
