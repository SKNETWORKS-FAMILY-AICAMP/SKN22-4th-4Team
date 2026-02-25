import os
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer


class FinBERTSentimentAnalyzer:
    """
    Hugging Face의 ProsusAI/finbert 모델을 다운로드/로드하여
    금융 텍스트의 감성(Positive/Negative/Neutral)을 분석하는 유틸리티 클래스입니다.
    """

    _instance = None
    _pipeline = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FinBERTSentimentAnalyzer, cls).__new__(cls)
            cls._instance._model_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "models", "finbert_model"
            )
            cls._instance._initialize_model()
        return cls._instance

    def _initialize_model(self):
        """로컬에 모델이 있으면 로드하고, 없으면 허깅페이스에서 다운로드 후 저장합니다."""
        if self._pipeline is None:
            if not os.path.exists(self._model_path):
                print(
                    f"Downloading FinBERT model to {self._model_path}... (This may take a while on first run)"
                )
                # 허깅페이스에서 모델과 토크나이저 다운로드
                model_name = "ProsusAI/finbert"
                model = AutoModelForSequenceClassification.from_pretrained(model_name)
                tokenizer = AutoTokenizer.from_pretrained(model_name)

                # 로컬 폴더에 저장
                os.makedirs(self._model_path, exist_ok=True)
                model.save_pretrained(self._model_path)
                tokenizer.save_pretrained(self._model_path)
                print("FinBERT model downloaded and saved locally.")
            else:
                print(f"Loading FinBERT model from local directory: {self._model_path}")

            # 로컬 경로에서 파이프라인 로드
            self._pipeline = pipeline(
                "sentiment-analysis", model=self._model_path, tokenizer=self._model_path
            )
            print("FinBERT model loaded successfully.")

    def analyze(self, text: str) -> dict:
        """
        텍스트의 감성을 분석하여 라벨과 신뢰도를 반환합니다.

        Args:
            text (str): 분석할 금융 텍스트 (예: 뉴스 헤드라인, 공시 요약)

        Returns:
            dict: {'label': 'positive'|'negative'|'neutral', 'score': float}
        """
        if not text or not text.strip():
            return {"label": "neutral", "score": 0.0}

        try:
            # 텍스트가 너무 긴 경우 잘라냄 (BERT모델 토큰 길이 제한 방지)
            truncated_text = text[:512]

            result = self._pipeline(truncated_text)[0]
            # 모델 출력(positive, negative, neutral)
            return {"label": result["label"].lower(), "score": float(result["score"])}
        except Exception as e:
            print(f"Sentiment analysis failed: {e}")
            return {"label": "neutral", "score": 0.0}
