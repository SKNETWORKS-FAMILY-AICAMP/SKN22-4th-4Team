"""
RAG System Evaluation Script using Ragas
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime

# Django settings setup (required to load AnalystChatbot which depends on Django models/settings)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

from datasets import Dataset
from ragas import evaluate
from ragas.metrics import (
    faithfulness,
    answer_relevancy,
    context_precision,
    context_recall,
)
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings

from src.rag.analyst_chat import AnalystChatbot


def main():
    print("🚀 Starting RAG Automated Evaluation...")

    # 1. Load Dataset
    dataset_path = os.path.join(
        os.path.dirname(__file__), "..", "04_test_plan_results", "rag_eval_dataset.csv"
    )
    if not os.path.exists(dataset_path):
        print(f"❌ Dataset not found at {dataset_path}")
        return

    df = pd.read_csv(dataset_path)
    questions = df["question"].tolist()
    ground_truths = df["ground_truth"].tolist()

    # 2. Initialize Chatbot
    try:
        chatbot = AnalystChatbot()
        print(f"✅ AnalystChatbot Initialized (Model: {chatbot.model})")
    except Exception as e:
        print(f"❌ Failed to initialize Chatbot: {e}")
        return

    # 3. Generate Answers and Contexts
    answers = []
    contexts = []

    print("\n🤖 Generating answers from Chatbot...")
    for idx, q in enumerate(questions):
        print(f"  [{idx+1}/{len(questions)}] Question: {q}")
        try:
            # Call the chatbot
            response = chatbot.chat(message=q, use_rag=True)

            # Extract answer and context
            answers.append(response.get("content", ""))

            # Context is usually a single string in AnalystChatbot, but Ragas expects a list of strings
            ctx_str = response.get("context", "")
            if ctx_str and ctx_str != "추가 컨텍스트 없음":
                # Split by the separator used in AnalystChatbot
                ctx_list = [c.strip() for c in ctx_str.split("---") if c.strip()]
            else:
                ctx_list = ["No context retrieved"]

            contexts.append(ctx_list)

        except Exception as e:
            print(f"    ❌ Error generating answer: {e}")
            answers.append("Error")
            contexts.append([])

        # Clear history to ensure independent tests
        chatbot.clear_history()

    # 4. Prepare huggingface Dataset for Ragas
    data = {
        "question": questions,
        "answer": answers,
        "contexts": contexts,
        "ground_truth": ground_truths,
    }
    dataset = Dataset.from_dict(data)

    # 5. Evaluate using Ragas
    print(
        "\n📊 Evaluating metrics with Ragas (This might take a while depending on the API)..."
    )

    # Setup LLM and Embeddings for evaluation
    # Using Gemini for evaluation if defined, fallback to OpenAI
    eval_llm = None
    eval_embeddings = None

    if os.getenv("GOOGLE_API_KEY"):
        eval_llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash")
        eval_embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
    elif os.getenv("OPENAI_API_KEY"):
        eval_llm = ChatOpenAI(model="gpt-4o-mini")
        eval_embeddings = OpenAIEmbeddings()
    else:
        print(
            "❌ API key (GOOGLE_API_KEY or OPENAI_API_KEY) must be set for Ragas evaluation."
        )
        return

    try:
        result = evaluate(
            dataset=dataset,
            metrics=[
                faithfulness,
                answer_relevancy,
                context_precision,
                context_recall,
            ],
            llm=eval_llm,
            embeddings=eval_embeddings,
        )

        # 6. Save and Display Results
        output_dir = os.path.join(
            os.path.dirname(__file__), "..", "04_test_plan_results"
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save summary
        summary_path = os.path.join(output_dir, f"rag_eval_summary_{timestamp}.json")
        with open(summary_path, "w") as f:
            # result is a dict-like object
            json.dump(dict(result), f, indent=4)

        # Save detailed dataframe
        df_result = result.to_pandas()
        csv_path = os.path.join(output_dir, f"rag_eval_detailed_{timestamp}.csv")
        df_result.to_csv(csv_path, index=False)

        print("\n✨ Evaluation Complete! ✨")
        print("=" * 40)
        print("SUMMARY SCORES:")
        for metric, score in dict(result).items():
            print(f" - {metric}: {score:.4f}")
        print("=" * 40)
        print(f"📄 Detailed results saved to: {csv_path}")
        print(f"📊 Summary saved to: {summary_path}")

    except Exception as e:
        print(f"\n❌ Error during Ragas evaluation: {e}")


if __name__ == "__main__":
    main()
