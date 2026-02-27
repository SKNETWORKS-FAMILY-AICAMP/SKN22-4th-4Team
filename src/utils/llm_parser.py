import json
import re
import logging
from typing import Tuple, List

logger = logging.getLogger(__name__)


def parse_llm_json_response(raw_content: str) -> Tuple[str, List[str]]:
    """
    LLM의 응답에서 JSON 형식을 파싱하여 답변(answer)과 추천 질문(recommendations)을 추출합니다.
    (4단계 폴백 적용: json.loads -> 중괄호 블록 추출 -> regex -> 수동 추출)
    """
    assistant_message = ""
    recommendations = []

    # Remove code block formatting if Gemini/OpenAI returned it wrapped in ```json ... ```
    clean_content = raw_content.strip()
    if clean_content.startswith("```json"):
        clean_content = clean_content[7:]
    if clean_content.startswith("```"):
        clean_content = clean_content[3:]
    if clean_content.endswith("```"):
        clean_content = clean_content[:-3]
    clean_content = clean_content.strip()

    # 1단계: 직접 json.loads
    try:
        parsed_content = json.loads(clean_content)
        assistant_message = parsed_content.get("answer", "")
        recommendations = parsed_content.get("recommendations", [])
        logger.info(
            f"JSON parsed successfully, answer length: {len(assistant_message)}"
        )
    except json.JSONDecodeError:
        logger.warning("JSON parsing failed, trying block extraction")

        # 2단계: 중괄호 블록 추출 후 json.loads 재시도
        parsed = False
        brace_start = clean_content.find("{")
        brace_end = clean_content.rfind("}")
        if brace_start != -1 and brace_end > brace_start:
            json_block = clean_content[brace_start : brace_end + 1]
            try:
                parsed_content = json.loads(json_block)
                assistant_message = parsed_content.get("answer", "")
                recommendations = parsed_content.get("recommendations", [])
                logger.info(
                    f"JSON block parsed successfully, answer length: {len(assistant_message)}"
                )
                parsed = True
            except json.JSONDecodeError:
                pass

        if not parsed:
            logger.warning("Block extraction failed, trying regex fallback")
            # 3단계: Regex fallback — "answer" 필드를 greedy하게 추출
            # 마지막 recommendations 또는 닫는 중괄호 전까지 모든 내용을 캡처
            answer_match = re.search(
                r'"answer"\s*:\s*"(.*?)"\s*,\s*"recommendations"',
                raw_content,
                re.DOTALL,
            )
            if not answer_match:
                # recommendations 없이 answer만 있는 경우 — 마지막 "} 까지 greedy 매칭
                answer_match = re.search(
                    r'"answer"\s*:\s*"(.*)"',
                    raw_content,
                    re.DOTALL,
                )

            if answer_match:
                assistant_message = answer_match.group(1)
                # 끝에 남는 닫는 패턴 정리
                for tail in ['"\n}', '",\n}', '"}', '", }', '" }', '",}']:
                    if assistant_message.endswith(tail):
                        assistant_message = assistant_message[: -len(tail)]
                        break
                # 마지막 불필요한 따옴표 제거
                assistant_message = assistant_message.rstrip('"').rstrip()
                # JSON 이스케이프 문자 복원
                assistant_message = assistant_message.replace("\\n", "\n")
                assistant_message = assistant_message.replace("\\t", "\t")
                assistant_message = assistant_message.replace('\\"', '"')
                assistant_message = assistant_message.replace("\\\\", "\\")
                logger.info(f"Regex extracted answer, length: {len(assistant_message)}")
            else:
                # 4단계: 최종 폴백 — "answer": 이후 텍스트를 뒤에서부터 잘라내기
                if '"answer"' in raw_content:
                    start_idx = raw_content.find('"answer"')
                    colon_idx = raw_content.find(":", start_idx)
                    if colon_idx != -1:
                        rest = raw_content[colon_idx + 1 :].strip()
                        if rest.startswith('"'):
                            rest = rest[1:]
                        # 뒤에서부터 마지막 닫는 중괄호를 찾아 그 앞의 따옴표까지 제거
                        last_brace = rest.rfind("}")
                        if last_brace != -1:
                            rest = rest[:last_brace].rstrip().rstrip(",").rstrip()
                            if rest.endswith('"'):
                                rest = rest[:-1]
                        assistant_message = (
                            rest.replace("\\n", "\n")
                            .replace('\\"', '"')
                            .replace("\\\\", "\\")
                        )
                        logger.info(
                            f"Manual extraction, length: {len(assistant_message)}"
                        )
                    else:
                        assistant_message = raw_content
                else:
                    assistant_message = raw_content

    # 만약 assistant_message가 여전히 비어있으면 raw_content 사용
    if not assistant_message.strip():
        assistant_message = raw_content

    return assistant_message, recommendations
