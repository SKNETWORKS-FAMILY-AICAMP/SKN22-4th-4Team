@echo off
chcp 65001 >nul
REM ─────────────────────────────────────────────────────────────────────────────
REM  로컬 개발 환경 (Windows): Redis + Celery Worker + Django 서버 시작
REM  사용법: scripts\start_dev.bat
REM ─────────────────────────────────────────────────────────────────────────────

echo ═══════════════════════════════════════════════════════
echo   Finance App - 개발 서버 시작 (Windows)
echo ═══════════════════════════════════════════════════════
echo.

REM ── 1. Redis 확인 ──────────────────────────────────────────────────────────
echo [1/3] Redis 확인 중...

REM Docker로 Redis 실행 (Windows에서 가장 간단한 방법)
docker ps -q -f name=finance_redis_dev >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    ✅ Redis Docker 컨테이너가 이미 실행 중입니다.
) else (
    echo    🚀 Redis Docker 컨테이너 시작 중...
    docker start finance_redis_dev >nul 2>&1
    if %ERRORLEVEL% NEQ 0 (
        echo    📦 새 Redis 컨테이너 생성 중...
        docker run -d --name finance_redis_dev -p 6379:6379 redis:7-alpine
        if %ERRORLEVEL% NEQ 0 (
            echo.
            echo    ❌ Docker가 실행되고 있지 않거나 설치되지 않았습니다.
            echo       아래 방법 중 하나를 사용하세요:
            echo.
            echo       방법 1) Docker Desktop 설치 후 다시 실행
            echo              https://www.docker.com/products/docker-desktop
            echo.
            echo       방법 2) Memurai (Windows 전용 Redis 대체) 설치
            echo              https://www.memurai.com/
            echo.
            pause
            exit /b 1
        )
    )
    timeout /t 2 /nobreak >nul
    echo    ✅ Redis 시작 완료 (localhost:6379)
)

REM ── 2. Celery Worker 시작 ──────────────────────────────────────────────────
echo.
echo [2/3] Celery Worker 시작 중...
echo    (새 CMD 창이 열립니다)
start "Celery Worker" cmd /k "cd /d %~dp0.. && celery -A config worker --loglevel=info --queues=heavy,default --pool=threads --concurrency=4"
timeout /t 3 /nobreak >nul
echo    ✅ Celery Worker 시작 완료

REM ── 3. Django 서버 시작 ────────────────────────────────────────────────────
echo.
echo [3/3] Django 개발 서버 시작...
echo    📡 서버 주소: http://localhost:8000
echo.
echo    종료하려면 이 창에서 Ctrl+C 를 누르세요.
echo    Celery Worker는 별도 CMD 창을 닫아주세요.
echo ═══════════════════════════════════════════════════════
echo.

cd /d %~dp0..
python manage.py runserver
