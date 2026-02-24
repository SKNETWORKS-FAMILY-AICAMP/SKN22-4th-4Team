import logging

logger = logging.getLogger(__name__)


def scheduler_status(request):
    """
    Context processor to inject APScheduler status into every template.
    Allows displaying 'S&P 500 Daily Collection' status in the sidebar.
    """
    try:
        from src.tools.scheduler_manager import is_running, get_next_run_time

        return {
            "scheduler_running": is_running(),
            "scheduler_next_run": get_next_run_time(),
        }
    except Exception as e:
        logger.warning(f"Failed to get scheduler status in context processor: {e}")
        return {"scheduler_running": False, "scheduler_next_run": None}
