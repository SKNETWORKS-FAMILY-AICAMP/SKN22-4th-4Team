"""
RAG System Evaluation Script using Ragas
"""

import os
import sys

# Windows cp949 인코딩 이모지 출력 문제 해결
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd
import json
import math
from datetime import datetime

# Django settings setup (required to load AnalystChatbot which depends on Django models/settings)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

from ragas import evaluate, EvaluationDataset, SingleTurnSample
from ragas.metrics import (
    Faithfulness,
    ResponseRelevancy,
    LLMContextPrecisionWithReference,
    LLMContextRecall,
)
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper

from src.rag.analyst_chat import AnalystChatbot


def main():
    # 1. Load Evaluation Dataset
    eval_csv = os.path.join(
        os.path.dirname(__file__), "..", "04_test_plan_results", "rag_eval_dataset.csv"
    )
    df = pd.read_csv(eval_csv)
    questions = df["question"].tolist()
    ground_truths = df["ground_truth"].tolist()

    print(f"📋 Loaded {len(questions)} evaluation questions from: {eval_csv}")

    # 2. Initialize Chatbot
    try:
        chatbot = AnalystChatbot()
        print(f"✅ AnalystChatbot Initialized (Model: {chatbot.model})")
    except Exception as e:
        print(f"❌ Failed to initialize Chatbot: {e}")
        return

    # 3. Generate Answers and Contexts → build SingleTurnSamples
    samples = []

    print("\n🤖 Generating answers from Chatbot...")
    for idx, q in enumerate(questions):
        print(f"  [{idx+1}/{len(questions)}] Question: {q}")
        try:
            # Call the chatbot
            response = chatbot.chat(message=q, use_rag=True)

            # Extract answer and context
            answer = response.get("content", "")

            # Context is usually a single string in AnalystChatbot, but Ragas expects a list of strings
            ctx_str = response.get("context", "")
            if ctx_str and ctx_str != "추가 컨텍스트 없음":
                # Split by the separator used in AnalystChatbot
                ctx_list = [c.strip() for c in ctx_str.split("---") if c.strip()]
            else:
                ctx_list = ["No context retrieved"]

            # Build SingleTurnSample (Ragas v0.2 format)
            sample = SingleTurnSample(
                user_input=q,
                response=answer,
                retrieved_contexts=ctx_list,
                reference=ground_truths[idx],
            )
            samples.append(sample)

        except Exception as e:
            print(f"    ❌ Error generating answer: {e}")
            sample = SingleTurnSample(
                user_input=q,
                response="Error",
                retrieved_contexts=[],
                reference=ground_truths[idx],
            )
            samples.append(sample)

        # Clear history to ensure independent tests
        chatbot.clear_history()

    # 4. Prepare EvaluationDataset for Ragas v0.2
    eval_dataset = EvaluationDataset(samples=samples)

    # 5. Evaluate using Ragas
    print(
        "\n📊 Evaluating metrics with Ragas (This might take a while depending on the API)..."
    )

    # Setup LLM and Embeddings for evaluation
    # Note: Google embedding models are deprecated. Using OpenAI embeddings.
    eval_embeddings = None
    eval_llm = None

    # Embeddings: 항상 OpenAI 사용 (Google embedding 모델이 폐기됨)
    if os.getenv("OPENAI_API_KEY"):
        eval_embeddings = LangchainEmbeddingsWrapper(
            OpenAIEmbeddings(model="text-embedding-3-small")
        )
    else:
        print("❌ OPENAI_API_KEY must be set for Ragas evaluation embeddings.")
        return

    # LLM: Gemini 우선, OpenAI 폴백
    if os.getenv("GOOGLE_API_KEY"):
        eval_llm = LangchainLLMWrapper(ChatGoogleGenerativeAI(model="gemini-2.5-flash"))
    elif os.getenv("OPENAI_API_KEY"):
        eval_llm = LangchainLLMWrapper(ChatOpenAI(model="gpt-4o-mini"))
    else:
        print(
            "❌ API key (GOOGLE_API_KEY or OPENAI_API_KEY) must be set for Ragas evaluation."
        )
        return

    # Initialize metrics with LLM (Ragas v0.2 style)
    metrics = [
        Faithfulness(llm=eval_llm),
        ResponseRelevancy(llm=eval_llm, embeddings=eval_embeddings),
        LLMContextPrecisionWithReference(llm=eval_llm),
        LLMContextRecall(llm=eval_llm),
    ]

    try:
        result = evaluate(
            dataset=eval_dataset,
            metrics=metrics,
        )

        # 6. Save and Display Results
        output_dir = os.path.join(
            os.path.dirname(__file__), "..", "04_test_plan_results"
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Convert result to pandas DataFrame
        df_result = result.to_pandas()

        # Calculate summary scores from DataFrame
        metric_names = [
            "faithfulness",
            "answer_relevancy",
            "context_precision",
            "context_recall",
        ]
        result_dict = {}
        for m in metric_names:
            if m in df_result.columns:
                mean_val = df_result[m].mean()
                result_dict[m] = (
                    None
                    if (isinstance(mean_val, float) and math.isnan(mean_val))
                    else float(mean_val)
                )
            else:
                result_dict[m] = None

        # Save summary
        summary_path = os.path.join(output_dir, f"rag_eval_summary_{timestamp}.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(result_dict, f, indent=4, ensure_ascii=False)

        # Save detailed dataframe
        csv_path = os.path.join(output_dir, f"rag_eval_detailed_{timestamp}.csv")
        df_result.to_csv(csv_path, index=False, encoding="utf-8-sig")

        print("\n✨ Evaluation Complete! ✨")
        print("=" * 40)
        print("SUMMARY SCORES:")
        for metric, score in result_dict.items():
            if score is not None:
                print(f" - {metric}: {score:.4f}")
            else:
                print(f" - {metric}: N/A (not enough data)")
        print("=" * 40)
        print(f"📄 Detailed results saved to: {csv_path}")
        print(f"📊 Summary saved to: {summary_path}")

    except Exception as e:
        import traceback

        print(f"\n❌ Error during Ragas evaluation: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
