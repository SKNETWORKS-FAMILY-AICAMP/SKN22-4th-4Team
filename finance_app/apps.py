from django.apps import AppConfig
import logging
import sys

logger = logging.getLogger(__name__)


class FinanceAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "finance_app"

    def ready(self):
        # Prevent running twice (e.g. runserver auto-reloader)
        if "runserver" not in sys.argv:
            return

        # Avoid running in migration or other management commands
        import os

        if os.environ.get("RUN_MAIN", None) != "true":
            return

        try:
            from src.tools.scheduler_manager import init_scheduler

            logger.info("Initializing S&P 500 Scheduler on Django Startup...")
            init_scheduler()
            logger.info("Scheduler Initialization Complete.")
        except Exception as e:
            logger.error(f"Failed to initialize scheduler: {e}")
