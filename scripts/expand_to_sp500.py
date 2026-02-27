"""
S&P 500 전체 기업으로 데이터베이스 확장
현재 Top 100에서 S&P 500 전체로 확장합니다.

데이터 소스:
- S&P 500 목록: 위키피디아
- CIK 정보: SEC EDGAR API
- 기업 상세 정보: Finnhub API
- 한글 이름: OpenAI API 번역
"""

import os
import sys
import time
import pandas as pd
import requests
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.supabase_client import SupabaseClient
from src.data.finnhub_client import FinnhubClient

load_dotenv()

# OpenAI 클라이언트
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_sp500_tickers() -> pd.DataFrame:
    """S&P 500 기업 리스트를 위키피디아에서 가져오기"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    try:
        # User-Agent 헤더 추가 (403 방지)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # 위키피디아 테이블 읽기
        tables = pd.read_html(response.text)
        sp500_table = tables[0]

        # 필요한 컬럼만 선택
        sp500_df = sp500_table[
            [
                "Symbol",
                "Security",
                "GICS Sector",
                "GICS Sub-Industry",
                "Headquarters Location",
            ]
        ]
        sp500_df.columns = [
            "ticker",
            "company_name",
            "sector",
            "industry",
            "headquarters",
        ]

        # 티커 정리 (점 제거 등)
        sp500_df["ticker"] = sp500_df["ticker"].str.replace(".", "-", regex=False)

        print(f"✅ S&P 500 기업 {len(sp500_df)}개 로드 완료")
        return sp500_df

    except Exception as e:
        print(f"❌ S&P 500 리스트 가져오기 실패: {e}")
        return pd.DataFrame()


def get_cik_map() -> Dict[str, str]:
    """SEC에서 티커-CIK 매핑 가져오기"""
    print("📋 SEC에서 CIK 매핑 로드 중...")

    headers = {
        "User-Agent": os.getenv("SEC_API_USER_AGENT", "researcher@university.edu"),
        "Accept": "application/json",
    }

    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        cik_map = {}
        for item in data.values():
            ticker = item.get("ticker", "").upper()
            cik = str(item.get("cik_str", "")).zfill(10)
            cik_map[ticker] = cik

        print(f"   {len(cik_map)}개 CIK 매핑 로드됨")
        return cik_map

    except Exception as e:
        print(f"⚠️  CIK 매핑 로드 실패: {e}")
        return {}


def translate_to_korean(company_name: str) -> str:
    """영문 회사명을 한글로 번역 (OpenAI 사용)"""
    try:
        response = openai_client.chat.completions.create(
            model=os.getenv("CHAT_MODEL", "gpt-4.1-mini"),
            messages=[
                {
                    "role": "system",
                    "content": "당신은 미국 기업명을 한글로 번역하는 전문가입니다. 널리 알려진 한글 표기가 있으면 그것을 사용하고, 없으면 발음을 한글로 표기하세요. 회사명만 반환하세요.",
                },
                {
                    "role": "user",
                    "content": f"다음 미국 기업명을 한글로 번역하세요: {company_name}",
                },
            ],
            max_completion_tokens=50,
            temperature=0.1,
        )
        korean_name = response.choices[0].message.content.strip()
        return korean_name
    except Exception as e:
        print(f"    ⚠️ 번역 실패: {e}")
        return None


def translate_batch(company_names: List[str], batch_size: int = 20) -> Dict[str, str]:
    """여러 회사명을 배치로 번역 (API 호출 최소화)"""
    translations = {}

    for i in range(0, len(company_names), batch_size):
        batch = company_names[i : i + batch_size]
        batch_text = "\n".join([f"{j+1}. {name}" for j, name in enumerate(batch)])

        try:
            response = openai_client.chat.completions.create(
                model=os.getenv("CHAT_MODEL", "gpt-4.1-mini"),
                messages=[
                    {
                        "role": "system",
                        "content": """당신은 미국 기업명을 한글로 번역하는 전문가입니다.
널리 알려진 한글 표기가 있으면 그것을 사용하고, 없으면 발음을 한글로 표기하세요.
각 줄에 번호와 한글 회사명만 반환하세요. 예: "1. 애플""",
                    },
                    {
                        "role": "user",
                        "content": f"다음 미국 기업명들을 한글로 번역하세요:\n{batch_text}",
                    },
                ],
                max_completion_tokens=1000,
                temperature=0.1,
            )

            result = response.choices[0].message.content.strip()
            lines = result.split("\n")

            for j, line in enumerate(lines):
                if j < len(batch):
                    # "1. 애플" 형식에서 한글명 추출
                    if ". " in line:
                        korean_name = line.split(". ", 1)[1].strip()
                    else:
                        korean_name = line.strip()
                    translations[batch[j]] = korean_name

            print(f"    📝 {i+1}~{min(i+batch_size, len(company_names))}개 번역 완료")
            time.sleep(0.5)  # Rate limit

        except Exception as e:
            print(f"    ⚠️ 배치 번역 실패: {e}")
            # 실패한 배치는 개별 번역 시도
            for name in batch:
                korean = translate_to_korean(name)
                if korean:
                    translations[name] = korean

    return translations


def get_existing_tickers() -> List[str]:
    """현재 DB에 있는 기업 티커 목록"""
    try:
        companies_df = SupabaseClient.get_all_companies()
        existing_tickers = companies_df["ticker"].tolist()
        print(f"📊 현재 DB에 {len(existing_tickers)}개 기업 존재")
        return existing_tickers
    except Exception as e:
        print(f"⚠️  기존 티커 조회 실패: {e}")
        return []


def get_missing_companies(
    sp500_df: pd.DataFrame, existing_tickers: List[str]
) -> pd.DataFrame:
    """아직 DB에 없는 기업 찾기"""
    missing_df = sp500_df[~sp500_df["ticker"].isin(existing_tickers)]
    print(f"🔍 추가할 기업: {len(missing_df)}개")
    return missing_df


def fetch_company_profile_from_finnhub(
    ticker: str, finnhub_client: FinnhubClient
) -> Optional[Dict]:
    """Finnhub에서 기업 프로필 데이터 가져오기"""
    try:
        profile = finnhub_client.get_company_profile(ticker)
        if profile:
            return {
                "ticker": ticker,
                "company_name": profile.get("name", ""),
                "sector": profile.get("finnhubIndustry", ""),
                "exchange": profile.get("exchange", ""),
                "market_cap": profile.get("marketCapitalization", 0),
                "logo_url": profile.get("logo", ""),  # logo -> logo_url
                "website": profile.get("weburl", ""),  # weburl -> website
            }
        return None
    except Exception as e:
        print(f"  ⚠️  {ticker} 프로필 조회 실패: {e}")
        return None


def add_companies_to_db(
    missing_df: pd.DataFrame,
    cik_map: Dict[str, str],
    korean_names: Dict[str, str],
    batch_size: int = 10,
):
    """새 기업을 DB에 추가"""
    finnhub_client = FinnhubClient()
    client = SupabaseClient.get_client()

    total = len(missing_df)
    success_count = 0
    fail_count = 0

    print(f"\n📤 {total}개 기업을 DB에 추가 중...")

    for idx, row in missing_df.iterrows():
        ticker = row["ticker"]
        company_name = row["company_name"]

        try:
            # Finnhub에서 상세 정보 가져오기
            profile = fetch_company_profile_from_finnhub(ticker, finnhub_client)

            # CIK 가져오기
            cik = cik_map.get(ticker) or cik_map.get(ticker.replace("-", ""))

            # 한글 이름 가져오기
            korean_name = korean_names.get(company_name)

            if profile:
                # DB에 삽입 (DB 스키마에 맞춤)
                company_data = {
                    "ticker": ticker,
                    "company_name": profile["company_name"] or company_name,
                    "cik": cik,
                    "sector": profile["sector"] or row["sector"],
                    "industry": row["industry"],
                    "exchange": profile["exchange"],
                    "market_cap": profile["market_cap"],
                    "logo_url": profile["logo_url"],
                    "website": profile["website"],
                    "headquarters": row.get("headquarters"),
                    "korean_name": korean_name,
                }
            else:
                # Finnhub 실패 시 기본 정보만 사용
                company_data = {
                    "ticker": ticker,
                    "company_name": company_name,
                    "cik": cik,
                    "sector": row["sector"],
                    "industry": row["industry"],
                    "headquarters": row.get("headquarters"),
                    "korean_name": korean_name,
                }

            result = client.table("companies").insert(company_data).execute()

            if result.data:
                success_count += 1
                kr_display = f" ({korean_name})" if korean_name else ""
                print(
                    f"  ✅ {success_count}/{total} - {ticker}: {company_data['company_name'][:30]}{kr_display}"
                )
            else:
                fail_count += 1
                print(f"  ❌ {ticker} 삽입 실패")

        except Exception as e:
            fail_count += 1
            print(f"  ❌ {ticker} 처리 중 오류: {e}")

        # API rate limit 방지
        if (idx + 1) % batch_size == 0:
            time.sleep(1)

    print(f"\n{'='*60}")
    print(f"✅ 성공: {success_count}개")
    print(f"❌ 실패: {fail_count}개")
    print(f"📊 총계: {success_count + fail_count}개 처리")
    print(f"{'='*60}\n")


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("🚀 S&P 500 데이터베이스 확장 시작")
    print("=" * 60)

    # 1. S&P 500 전체 리스트 가져오기 (위키피디아)
    sp500_df = get_sp500_tickers()
    if sp500_df.empty:
        print("❌ S&P 500 리스트를 가져올 수 없습니다.")
        return

    # 2. SEC에서 CIK 매핑 가져오기
    cik_map = get_cik_map()

    # 3. 현재 DB에 있는 티커 확인
    existing_tickers = get_existing_tickers()

    # 4. 추가할 기업 찾기
    missing_df = get_missing_companies(sp500_df, existing_tickers)

    if missing_df.empty:
        print("✅ 모든 S&P 500 기업이 이미 DB에 있습니다!")
        return

    # 5. 사용자 확인
    print(f"\n⚠️  {len(missing_df)}개 기업을 추가하시겠습니까? (y/n): ", end="")
    confirm = input().strip().lower()

    if confirm != "y":
        print("❌ 작업 취소됨")
        return

    # 6. 한글 이름 번역 (배치 처리)
    print("\n📝 한글 이름 번역 중...")
    company_names = missing_df["company_name"].tolist()
    korean_names = translate_batch(company_names)
    print(f"   {len(korean_names)}개 한글 이름 번역 완료")

    # 7. DB에 추가
    add_companies_to_db(missing_df, cik_map, korean_names)

    print("\n✅ S&P 500 확장 완료!")
    print(f"📊 최종 기업 수: {len(existing_tickers) + len(missing_df)}개 (예상)")
    print("\n💡 다음 단계:")
    print("   1. python scripts/collect_10k_relationships.py --source supabase")
    print("   2. python scripts/upload_relationships_to_supabase.py")


if __name__ == "__main__":
    main()
