"""
LLM Client - 통합 LLM 래퍼
Google Gemini와 OpenAI를 통합하는 클라이언트.
LangSmith 트레이싱을 자동 지원합니다.
"""

import os
import json
import logging
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv

try:
    from langsmith import traceable
except ImportError:
    # langsmith가 설치되지 않은 경우 no-op decorator
    def traceable(*args, **kwargs):
        def decorator(func):
            return func

        if args and callable(args[0]):
            return args[0]
        return decorator


load_dotenv()

logger = logging.getLogger(__name__)


class LLMClient:
    """
    LLM 통합 클라이언트
    - Gemini: 채팅/분석용 (무료)
    - OpenAI: 임베딩 전용
    """

    def __init__(self, model_name: Optional[str] = None):
        self.provider = os.getenv("LLM_PROVIDER", "openai").lower()
        self.model = model_name or os.getenv("CHAT_MODEL", "gpt-4.1-mini")
        self.temperature = float(os.getenv("TEMPERATURE", "0.1"))
        self.max_tokens = int(os.getenv("MAX_TOKENS", "4096"))

        if self.provider == "gemini":
            self._init_gemini()
        else:
            self._init_openai()

        logger.info(
            f"LLMClient initialized: provider={self.provider}, model={self.model}"
        )

    def _init_gemini(self):
        """Google Gemini 초기화"""
        try:
            from google import genai

            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                raise ValueError("GOOGLE_API_KEY 환경 변수가 필요합니다.")

            self.client = genai.Client(api_key=api_key)
            self._gemini_available = True
            logger.info(f"Gemini client initialized: {self.model}")
        except Exception as e:
            logger.error(f"Gemini 초기화 실패: {e}")
            raise

    def _init_openai(self):
        """OpenAI 초기화 (폴백용)"""
        from openai import OpenAI

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY 환경 변수가 필요합니다.")
        self.client = OpenAI(api_key=api_key)

    @traceable(run_type="llm", name="chat_completion")
    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        json_mode: bool = False,
    ) -> str:
        """
        채팅 완성 요청 (통합 인터페이스)

        Args:
            messages: [{"role": "system"|"user"|"assistant", "content": "..."}]
            temperature: 생성 온도
            max_tokens: 최대 토큰 수
            json_mode: JSON 모드 활성화 여부

        Returns:
            생성된 텍스트
        """
        temp = temperature if temperature is not None else self.temperature
        max_tok = max_tokens or self.max_tokens

        if self.provider == "gemini":
            return self._gemini_chat(messages, temp, max_tok, json_mode)
        else:
            return self._openai_chat(messages, temp, max_tok, json_mode)

    def _gemini_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """Gemini API로 채팅 완성"""
        from google.genai import types

        # 시스템 프롬프트와 사용자 메시지 분리
        system_instruction = None
        contents = []

        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            if role == "system":
                system_instruction = content
            elif role == "assistant":
                contents.append(
                    types.Content(
                        role="model", parts=[types.Part.from_text(text=content)]
                    )
                )
            else:  # user, tool 등
                contents.append(
                    types.Content(
                        role="user", parts=[types.Part.from_text(text=content)]
                    )
                )

        # Config 설정
        config_kwargs = {
            "temperature": temperature,
            "max_output_tokens": max_tokens,
        }

        if system_instruction:
            config_kwargs["system_instruction"] = system_instruction

        if json_mode:
            config_kwargs["response_mime_type"] = "application/json"

        config = types.GenerateContentConfig(**config_kwargs)

        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=config,
        )

        return response.text or ""

    def _openai_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """OpenAI API로 채팅 완성 (폴백)"""
        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = self.client.chat.completions.create(**kwargs)
        return response.choices[0].message.content or ""

    @traceable(run_type="llm", name="chat_completion_with_tools")
    def chat_completion_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        json_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        도구 호출을 포함한 채팅 완성 (통합 인터페이스)

        Returns:
            {
                "content": str | None,
                "tool_calls": [{"name": str, "arguments": dict, "id": str}] | None
            }
        """
        temp = temperature if temperature is not None else self.temperature
        max_tok = max_tokens or self.max_tokens

        if self.provider == "gemini":
            return self._gemini_chat_with_tools(
                messages, tools, temp, max_tok, json_mode
            )
        else:
            return self._openai_chat_with_tools(
                messages, tools, temp, max_tok, json_mode
            )

    def _gemini_chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict],
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> Dict[str, Any]:
        """Gemini API로 도구 호출 포함 채팅"""
        from google.genai import types

        # 시스템 프롬프트와 사용자 메시지 분리
        system_instruction = None
        contents = []

        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            if role == "system":
                system_instruction = content
            elif role == "assistant":
                contents.append(
                    types.Content(
                        role="model", parts=[types.Part.from_text(text=content)]
                    )
                )
            elif role == "tool":
                # 도구 결과를 user 메시지로 전달
                tool_name = msg.get("name", "tool")
                contents.append(
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_text(
                                text=f"[Tool Result: {tool_name}]\n{content}"
                            )
                        ],
                    )
                )
            else:
                contents.append(
                    types.Content(
                        role="user", parts=[types.Part.from_text(text=content)]
                    )
                )

        # OpenAI tools 형식 → Gemini FunctionDeclaration 변환
        gemini_tools = self._convert_tools_to_gemini(tools)

        config_kwargs = {
            "temperature": temperature,
            "max_output_tokens": max_tokens,
            "tools": gemini_tools,
        }

        if system_instruction:
            config_kwargs["system_instruction"] = system_instruction

        # Gemini에서는 tools + json_mode 동시 사용 불가
        # tool calling 시 json_mode 무시
        if json_mode and not gemini_tools:
            config_kwargs["response_mime_type"] = "application/json"

        config = types.GenerateContentConfig(**config_kwargs)

        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=config,
        )

        # 응답 파싱
        result = {"content": None, "tool_calls": None}

        if response.candidates and response.candidates[0].content:
            parts = response.candidates[0].content.parts
            tool_calls = []
            text_parts = []

            for part in parts:
                if part.function_call:
                    fc = part.function_call
                    tool_calls.append(
                        {
                            "name": fc.name,
                            "arguments": dict(fc.args) if fc.args else {},
                            "id": f"call_{fc.name}_{len(tool_calls)}",
                        }
                    )
                elif part.text:
                    text_parts.append(part.text)

            if tool_calls:
                result["tool_calls"] = tool_calls
            if text_parts:
                result["content"] = "\n".join(text_parts)

        return result

    def _openai_chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict],
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> Dict[str, Any]:
        """OpenAI API로 도구 호출 포함 채팅 (폴백)"""
        kwargs = {
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": temperature,
            "max_completion_tokens": max_tokens,
        }

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = self.client.chat.completions.create(**kwargs)
        resp_msg = response.choices[0].message

        result = {"content": resp_msg.content, "tool_calls": None}

        if resp_msg.tool_calls:
            result["tool_calls"] = [
                {
                    "name": tc.function.name,
                    "arguments": json.loads(tc.function.arguments),
                    "id": tc.id,
                }
                for tc in resp_msg.tool_calls
            ]

        return result

    def _convert_tools_to_gemini(self, openai_tools: List[Dict]) -> List:
        """OpenAI tool 형식을 Gemini FunctionDeclaration으로 변환"""
        from google.genai import types

        declarations = []

        for tool in openai_tools:
            if tool.get("type") != "function":
                continue

            func = tool["function"]
            params = func.get("parameters", {})

            # OpenAI 형식의 properties를 Gemini Schema로 변환
            properties = {}
            for prop_name, prop_def in params.get("properties", {}).items():
                prop_type = prop_def.get("type", "string").upper()
                type_map = {
                    "STRING": "STRING",
                    "INTEGER": "INTEGER",
                    "NUMBER": "NUMBER",
                    "BOOLEAN": "BOOLEAN",
                    "ARRAY": "ARRAY",
                    "OBJECT": "OBJECT",
                }
                schema_type = type_map.get(prop_type, "STRING")

                prop_schema = types.Schema(
                    type=schema_type,
                    description=prop_def.get("description", ""),
                )

                # enum 지원
                if "enum" in prop_def:
                    prop_schema = types.Schema(
                        type=schema_type,
                        description=prop_def.get("description", ""),
                        enum=prop_def["enum"],
                    )

                properties[prop_name] = prop_schema

            schema = types.Schema(
                type="OBJECT",
                properties=properties,
                required=params.get("required", []),
            )

            declaration = types.FunctionDeclaration(
                name=func["name"],
                description=func.get("description", ""),
                parameters=schema,
            )
            declarations.append(declaration)

        if declarations:
            return [types.Tool(function_declarations=declarations)]
        return []


# 싱글톤 패턴을 위한 인스턴스 캐시
_llm_client_instance: Optional[LLMClient] = None


def get_llm_client(model_name: Optional[str] = None) -> LLMClient:
    """LLM 클라이언트 싱글톤 반환"""
    global _llm_client_instance
    if _llm_client_instance is None or (
        model_name and _llm_client_instance.model != model_name
    ):
        _llm_client_instance = LLMClient(model_name=model_name)
    return _llm_client_instance


if __name__ == "__main__":
    print("🔄 LLM Client 테스트...")
    try:
        client = get_llm_client()
        print(f"✅ Provider: {client.provider}")
        print(f"   Model: {client.model}")

        # 간단 테스트
        result = client.chat_completion(
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant. Reply in Korean.",
                },
                {"role": "user", "content": "안녕하세요! 자기소개 한 줄만 해주세요."},
            ],
            max_tokens=100,
        )
        print(f"   응답: {result}")
    except Exception as e:
        print(f"❌ 오류: {e}")
