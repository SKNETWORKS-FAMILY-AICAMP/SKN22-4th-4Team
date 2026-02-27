# Celery 앱을 Django 시작 시 자동으로 불러오도록 설정
try:
    from .celery import app as celery_app

    __all__ = ("celery_app",)
except ImportError:
    pass
