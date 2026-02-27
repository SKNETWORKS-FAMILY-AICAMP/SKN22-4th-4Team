#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# 로컬 개발 환경: Redis + Celery Worker + Django 서버 일괄 시작 스크립트
# 사용법: bash scripts/start_dev.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "═══════════════════════════════════════════════════════"
echo "  Finance App - 개발 서버 시작 (Celery + Redis)"
echo "═══════════════════════════════════════════════════════"

# ── 1. Redis 확인 및 시작 ──────────────────────────────────────────────────
echo ""
echo "▶  [1/4] Redis 서버 확인..."
if command -v redis-server &>/dev/null; then
    if redis-cli ping &>/dev/null; then
        echo "   ✅ Redis가 이미 실행 중입니다."
    else
        echo "   🚀 Redis 시작 중..."
        redis-server --daemonize yes --bind 127.0.0.1 --port 6379
        sleep 1
        echo "   ✅ Redis 시작 완료 (localhost:6379)"
    fi
else
    echo "   ⚠️  redis-server 명령어를 찾을 수 없습니다."
    echo "       Homebrew로 설치: brew install redis"
    echo "       Docker로 대체 실행..."
    
    if command -v docker &>/dev/null; then
        docker run -d --name finance_redis_dev -p 6379:6379 redis:7-alpine 2>/dev/null || \
        docker start finance_redis_dev 2>/dev/null || true
        echo "   ✅ Redis Docker 컨테이너 시작 완료"
    else
        echo "   ❌ redis-server와 docker 모두 없습니다. Redis를 먼저 설치하세요."
        exit 1
    fi
fi

# ── 2. Log 디렉터리 생성 ──────────────────────────────────────────────────
mkdir -p "$PROJECT_DIR/logs"

# ── 3. Celery Worker 시작 (heavy 큐) ──────────────────────────────────────
echo ""
echo "▶  [2/4] Celery Worker (heavy 큐) 시작..."
celery -A config worker \
    --loglevel=info \
    --queues=heavy,default \
    --pool=threads \
    --concurrency=4 \
    --hostname=dev_worker@%h \
    --logfile="$PROJECT_DIR/logs/celery_worker.log" \
    --pidfile="$PROJECT_DIR/logs/celery_worker.pid" \
    --detach
echo "   ✅ Celery Worker 시작 완료 (로그: logs/celery_worker.log)"

# ── 4. Celery Flower 모니터링 대시보드 시작 (선택) ──────────────────────
echo ""
echo "▶  [3/4] Celery Flower 대시보드 시작..."
if command -v celery &>/dev/null; then
    celery -A config flower \
        --port=5555 \
        --logfile="$PROJECT_DIR/logs/flower.log" \
        --detach 2>/dev/null || echo "   ℹ️  Flower 시작 시도 (백그라운드)"
    echo "   ✅ Flower 대시보드: http://localhost:5555"
fi

# ── 5. Django 개발 서버 ────────────────────────────────────────────────────
echo ""
echo "▶  [4/4] Django 개발 서버 시작..."
echo "   📡 서버 주소: http://localhost:8000"
echo ""
echo "   중지하려면 Ctrl+C를 누르세요."
echo "   Celery Worker 중지: celery multi stop dev_worker --pidfile=logs/celery_worker.pid"
echo "═══════════════════════════════════════════════════════"
echo ""

python manage.py runserver
