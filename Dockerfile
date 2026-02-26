# Python 3.12 슬림 이미지 사용 (README 권장 버전에 맞춤)
FROM python:3.12-slim

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 의존성 설치 (빌드 및 DB 연결 등에 필요한 기본 패키지)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 요구사항 파일 복사 및 의존성 설치
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install gunicorn

# 프로젝트 코드 전체 복사 (.dockerignore에서 불필요한 파일 제외)
COPY . /app/

# 포트 노출
EXPOSE 8000

# WSGI 앱으로 Gunicorn 배포용 서버 실행
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "config.wsgi:application"]
