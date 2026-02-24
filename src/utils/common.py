"""
Common Import Utilities - 프로젝트 전역 유틸리티

중복되는 import 패턴과 초기화 로직을 중앙 관리합니다.
- 모듈 import fallback 패턴 통합
- 환경변수 로딩 중앙화
- 싱글톤 클라이언트 관리
"""

import os
import logging
from typing import Optional, TypeVar, Callable, Any
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

T = TypeVar("T")


def safe_import(
    primary_path: str,
    fallback_path: Optional[str] = None,
    attr_name: Optional[str] = None,
) -> Optional[Any]:
    """
    안전한 모듈 import (fallback 지원)

    Args:
        primary_path: 주 import 경로 (예: "rag.vector_store")
        fallback_path: 대체 경로 (예: "src.rag.vector_store")
        attr_name: 가져올 속성명 (None이면 모듈 전체)

    Returns:
        import된 모듈/객체 또는 None

    Example:
        VectorStore = safe_import("rag.vector_store", "src.rag.vector_store", "VectorStore")
    """
    import importlib

    for path in [primary_path, fallback_path]:
        if not path:
            continue
        try:
            module = importlib.import_module(path)
            if attr_name:
                return getattr(module, attr_name, None)
            return module
        except ImportError:
            continue

    return None


def import_with_fallback(*import_specs: tuple) -> dict:
    """
    여러 모듈을 fallback과 함께 일괄 import

    Args:
        import_specs: (primary_path, fallback_path, attr_name) 튜플들

    Returns:
        {attr_name: imported_object} 딕셔너리

    Example:
        modules = import_with_fallback(
            ("rag.vector_store", "src.rag.vector_store", "VectorStore"),
            ("rag.graph_rag", "src.rag.graph_rag", "GraphRAG"),
        )
        VectorStore = modules.get("VectorStore")
    """
    result = {}
    for spec in import_specs:
        if len(spec) == 3:
            primary, fallback, attr = spec
            obj = safe_import(primary, fallback, attr)
            if obj:
                result[attr] = obj
        elif len(spec) == 2:
            primary, attr = spec
            obj = safe_import(primary, None, attr)
            if obj:
                result[attr] = obj
    return result


def get_env_required(key: str, error_msg: Optional[str] = None) -> str:
    """
    필수 환경변수 로드 (없으면 예외 발생)

    Args:
        key: 환경변수 키
        error_msg: 커스텀 에러 메시지

    Returns:
        환경변수 값

    Raises:
        ValueError: 환경변수가 없는 경우
    """
    value = os.getenv(key)
    if not value:
        msg = error_msg or f"{key} 환경 변수가 필요합니다."
        raise ValueError(msg)
    return value


def get_env_optional(key: str, default: str = "") -> str:
    """선택적 환경변수 로드"""
    return os.getenv(key, default)


@lru_cache(maxsize=1)
def get_openai_client():
    """OpenAI 클라이언트 싱글톤"""
    from openai import OpenAI

    api_key = get_env_required("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)


@lru_cache(maxsize=1)
def get_supabase_client():
    """Supabase 클라이언트 싱글톤"""
    from supabase import create_client

    url = get_env_required("SUPABASE_URL")
    key = get_env_required("SUPABASE_KEY")
    return create_client(url, key)


def try_get_client(
    factory_func: Callable[[], T],
    client_name: str = "Client",
) -> Optional[T]:
    """
    클라이언트 생성 시도 (실패해도 예외 발생 안함)

    Args:
        factory_func: 클라이언트 생성 함수
        client_name: 로깅용 클라이언트 이름

    Returns:
        클라이언트 또는 None
    """
    try:
        return factory_func()
    except Exception as e:
        logger.warning(f"{client_name} initialization failed: {e}")
        return None


# 공통 상수
class ModelNames:
    """OpenAI 모델명 상수"""

    GPT4_MINI = "gpt-4.1-mini"
    GPT4 = "gpt-4"
    EMBEDDING = "text-embedding-3-small"


class TableNames:
    """Supabase 테이블명 상수"""

    COMPANIES = "companies"
    ANNUAL_REPORTS = "annual_reports"
    DOCUMENTS = "documents"
