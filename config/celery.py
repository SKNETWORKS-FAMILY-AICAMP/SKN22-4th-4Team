"""
Celery 설정 파일
Django 프로젝트에서 Celery를 사용하기 위한 설정
"""
import os
from celery import Celery

# Django settings 모듈 설정
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("finance_app")

# Django settings에서 CELERY_ 프리픽스로 시작하는 설정을 자동으로 읽음
app.config_from_object("django.conf:settings", namespace="CELERY")

# 등록된 모든 Django 앱에서 tasks.py를 자동으로 탐색
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
