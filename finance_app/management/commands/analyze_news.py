from django.core.management.base import BaseCommand
from src.services.news_analyzer import NewsAnalyzerService


class Command(BaseCommand):
    help = "Finnhub에서 주요 기업의 뉴스를 수집하고 FinBERT를 이용해 감성 분석 후 Supabase에 저장합니다."

    def handle(self, *args, **options):
        # 1. 서비스 초기화 및 파이프라인 실행
        try:
            service = NewsAnalyzerService()
            total_inserted = service.run_pipeline()

            self.stdout.write(
                self.style.SUCCESS(
                    f"\n[DONE] 파이프라인 종료. 총 {total_inserted}건의 뉴스 데이터가 성공적으로 분석 및 저장되었습니다."
                )
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"\n[ERROR] 파이프라인 실행 중 오류 발생: {e}")
            )
