"""
RAG 관련 모듈 공통 베이스 클래스
LLM(Gemini/OpenAI), Supabase, Finnhub 및 RAG 엔진의 공통 초기화 로직을 관리합니다.
LangSmith 트레이싱을 지원합니다.
"""

import os
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from openai import OpenAI
from supabase import create_client, Client

# 로깅 설정
logger = logging.getLogger(__name__)
load_dotenv()

# LLM Client 임포트
try:
    from rag.llm_client import get_llm_client, LLMClient
except ImportError:
    try:
        from src.rag.llm_client import get_llm_client, LLMClient
    except ImportError:
        get_llm_client = None
        LLMClient = None
        logger.warning("LLMClient not found. Falling back to OpenAI.")

# RAG 모듈 임포트
try:
    from rag.vector_store import VectorStore
    from rag.graph_rag import GraphRAG
    from rag.data_retriever import DataRetriever

    RAG_AVAILABLE = True
except ImportError:
    try:
        from src.rag.vector_store import VectorStore
        from src.rag.graph_rag import GraphRAG
        from src.rag.data_retriever import DataRetriever

        RAG_AVAILABLE = True
    except ImportError:
        RAG_AVAILABLE = False
        logger.warning("RAG core modules not found. Some features may be disabled.")

# Stock API 클라이언트 임포트
try:
    from data.stock_api_client import get_stock_api_client

    STOCK_API_AVAILABLE = True
except ImportError:
    try:
        from src.data.stock_api_client import get_stock_api_client

        STOCK_API_AVAILABLE = True
    except ImportError:
        STOCK_API_AVAILABLE = False

# 환율 클라이언트 임포트
try:
    from tools.exchange_rate_client import get_exchange_client

    EXCHANGE_AVAILABLE = True
except ImportError:
    try:
        from src.tools.exchange_rate_client import get_exchange_client

        EXCHANGE_AVAILABLE = True
    except ImportError:
        EXCHANGE_AVAILABLE = False


class RAGBase:
    """RAG 시스템의 공통 클라이언트 및 데이터베이스 연결을 관리하는 베이스 클래스"""

    def __init__(self, model_name: Optional[str] = None):
        # 0. LangSmith 트레이싱 확인
        if os.getenv("LANGSMITH_TRACING", "").lower() == "true":
            logger.info(
                f"LangSmith tracing enabled: project={os.getenv('LANGSMITH_PROJECT', 'default')}"
            )

        # 1. LLM Client 초기화 (Gemini 또는 OpenAI)
        resolved_model = model_name or os.getenv("CHAT_MODEL", "gemini-2.5-flash")
        self.model = resolved_model

        if get_llm_client is not None:
            self.llm_client = get_llm_client(resolved_model)
            logger.info(
                f"LLM Client: {self.llm_client.provider}/{self.llm_client.model}"
            )
        else:
            self.llm_client = None
            logger.warning("LLM Client unavailable, using OpenAI fallback only")

        # 2. OpenAI 초기화 (임베딩 전용)
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_client = None
        if self.openai_api_key:
            self.openai_client = OpenAI(api_key=self.openai_api_key)
        self.embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

        # 3. Supabase 초기화
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL과 SUPABASE_KEY 환경 변수가 필요합니다.")
        self.supabase: Client = create_client(supabase_url, supabase_key)

        # 4. Stock API 초기화
        self.finnhub = None
        if STOCK_API_AVAILABLE:
            try:
                self.finnhub = get_stock_api_client()
                if not self.finnhub.api_key:
                    self.finnhub = None
            except Exception as e:
                logger.warning(f"Stock API init failed: {e}")

        # 5. RAG 엔진 초기화
        self.vector_store = None
        self.graph_rag = None
        self.data_retriever = None

        if RAG_AVAILABLE:
            try:
                self.vector_store = VectorStore()
                self.graph_rag = GraphRAG()
                self.data_retriever = DataRetriever(
                    supabase=self.supabase,
                    vector_store=self.vector_store,
                    graph_rag=self.graph_rag,
                    finnhub=self.finnhub,
                )
                logger.info(
                    "RAG Engine (VectorStore, GraphRAG, DataRetriever) initialized"
                )
            except Exception as e:
                logger.warning(f"RAG Engine init failed: {e}")

    def _llm_chat(self, messages, temperature=None, max_tokens=None, json_mode=False):
        """통합 LLM 채팅 호출 (Gemini 우선, OpenAI 폴백)"""
        if self.llm_client:
            result = self.llm_client.chat_completion(
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                json_mode=json_mode,
            )
            return result or ""
        elif self.openai_client:
            # OpenAI 폴백
            _restricted = {"gpt-5-nano", "o1-mini", "o1-preview", "o1", "o3-mini"}
            kwargs = {
                "model": self.model,
                "messages": messages,
            }
            if self.model not in _restricted:
                kwargs["temperature"] = temperature or 0.1
                kwargs["max_completion_tokens"] = max_tokens or 4096
            if json_mode:
                kwargs["response_format"] = {"type": "json_object"}
            response = self.openai_client.chat.completions.create(**kwargs)
            return response.choices[0].message.content or ""
        else:
            raise RuntimeError("No LLM client available")

    def _llm_chat_stream(self, messages, temperature=None, max_tokens=None):
        """통합 LLM 채팅 호출 (Streaming)"""
        if self.llm_client:
            yield from self.llm_client.chat_completion_stream(
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        elif self.openai_client:
            _restricted = {"gpt-5-nano", "o1-mini", "o1-preview", "o1", "o3-mini"}
            kwargs = {
                "model": self.model,
                "messages": messages,
                "stream": True,
            }
            if self.model not in _restricted:
                kwargs["temperature"] = temperature or 0.1
                kwargs["max_completion_tokens"] = max_tokens or 4096
            response = self.openai_client.chat.completions.create(**kwargs)
            for chunk in response:
                if chunk.choices and len(chunk.choices) > 0:
                    yield chunk.choices[0].delta.content or ""
        else:
            raise RuntimeError("No LLM client available")

    def _load_prompt(self, filename: str) -> str:
        """프롬프트 파일 로드"""
        prompts_dir = Path(__file__).parent.parent / "prompts"
        prompt_path = prompts_dir / filename

        if not prompt_path.exists():
            # Alternative path check
            prompt_path = (
                Path(__file__).parent.parent.parent / "src" / "prompts" / filename
            )

        if prompt_path.exists():
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        logger.error(f"Prompt file not found: {filename}")
        return ""
