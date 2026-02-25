import json
import re
import logging
from typing import Tuple, List

logger = logging.getLogger(__name__)


def parse_llm_json_response(raw_content: str) -> Tuple[str, List[str]]:
    """
    LLM의 응답에서 JSON 형식을 파싱하여 답변(answer)과 추천 질문(recommendations)을 추출합니다.
    (3단계 폴백 적용: json.loads -> regex -> 수동 추출)
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

    try:
        parsed_content = json.loads(clean_content)
        assistant_message = parsed_content.get("answer", "")
        recommendations = parsed_content.get("recommendations", [])
        logger.info(
            f"JSON parsed successfully, answer length: {len(assistant_message)}"
        )
    except json.JSONDecodeError:
        logger.warning("JSON parsing failed, trying regex fallback")
        # Regex fallback: "answer" 필드만 추출
        answer_match = re.search(
            r'"answer"\s*:\s*"((?:[^"\\]|\\.)*)"\s*[,}]',
            raw_content,
            re.DOTALL,
        )
        if answer_match:
            assistant_message = answer_match.group(1)
            # JSON 이스케이프 문자 복원
            assistant_message = assistant_message.replace("\\n", "\n")
            assistant_message = assistant_message.replace("\\t", "\t")
            assistant_message = assistant_message.replace('\\"', '"')
            assistant_message = assistant_message.replace("\\\\", "\\")
            logger.info(f"Regex extracted answer, length: {len(assistant_message)}")
        else:
            # 최종 폴백: raw_content가 JSON처럼 보이면 "answer": 이후 텍스트 추출 시도
            if '"answer"' in raw_content:
                # { "answer": "..." } 형태에서 내용만 추출
                start_idx = raw_content.find('"answer"')
                colon_idx = raw_content.find(":", start_idx)
                if colon_idx != -1:
                    rest = raw_content[colon_idx + 1 :].strip()
                    if rest.startswith('"'):
                        rest = rest[1:]
                    # 마지막 닫는 따옴표+중괄호 제거
                    for end_pattern in ['"}', '"\n}', '" }']:
                        if rest.endswith(end_pattern):
                            rest = rest[: -len(end_pattern)]
                            break
                    assistant_message = rest.replace("\\n", "\n").replace('\\"', '"')
                    logger.info(f"Manual extraction, length: {len(assistant_message)}")
                else:
                    assistant_message = raw_content
            else:
                assistant_message = raw_content

    # 만약 assistant_message가 여전히 비어있으면 raw_content 사용
    if not assistant_message.strip():
        assistant_message = raw_content

    return assistant_message, recommendations
