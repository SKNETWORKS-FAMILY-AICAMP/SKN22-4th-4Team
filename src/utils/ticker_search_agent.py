import os
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser


def find_ticker_from_web(query: str) -> tuple[str, str | None]:
    """
    Searches the web for the stock ticker of a given company/term.
    Uses Tavily API for search and GPT to extract the ticker.

    Args:
        query (str): Company name or term (e.g., "Hearthstone", "Galax")

    Returns:
        tuple[str, str | None]: (Resolved Ticker, Reason/Explanation)
        Example: ("MSFT", "Activision Blizzard was acquired by Microsoft.")
    """
    api_key = os.environ.get("TAVILY_API_KEY")
    if not api_key:
        print("Warning: TAVILY_API_KEY not found. Skipping web search.")
        return "UNKNOWN", None

    try:
        # 1. Search Web - Request specifically about stock status and hierarchy
        search = TavilySearchResults(k=3, tavily_api_key=api_key)
        # Search specifically if acquired or delisted
        search_query = f"What is the current stock ticker for '{query}'? If acquired or delisted, who owns it and what is their ticker?"
        results = search.invoke(search_query)

        # 2. Extract Ticker using LLM
        # We use a small model for speed and cost efficiency
        llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0)

        prompt = ChatPromptTemplate.from_template(
            """
            Analyze the search results to identify the **currently active** stock ticker for "{query}".
            
            Search Results:
            {results}
            
            **CRITICAL RULES:**
            1. **Acquired/Subsidiary**: If "{query}" was acquired (e.g., Activision Blizzard by Microsoft), return the **PARENT company's ticker** (e.g., "MSFT").
            2. **Delisted/Private**: If the company is private or delisted and has no public parent, return "UNKNOWN".
            3. **Format**: Return ONLY the ticker symbol and a short reason separated by a pipe `|`.
               Format: `TICKER|Reason`
               Example: `MSFT|Blizzard is a subsidiary of Microsoft`
            4. **Prioritize US Listings**: Prefer US market tickers (NYSE/NASDAQ) if multiple exist.
            
            Examples:
            - "Blizzard" -> "MSFT|Blizzard was acquired by Microsoft"
            - "YouTube" -> "GOOGL|YouTube is a subsidiary of Alphabet Inc."
            - "Starlink" -> "UNKNOWN|SpaceX is a private company"
            """
        )

        chain = prompt | llm | StrOutputParser()
        result_text = chain.invoke({"query": query, "results": results})

        # Parse Ticker|Reason
        if "|" in result_text:
            parts = result_text.split("|", 1)
            ticker = parts[0].strip().upper()
            reason = parts[1].strip()
        else:
            ticker = result_text.strip().upper()
            reason = None

        # Basic validation: Tickers are usually short alphanumeric
        if len(ticker) > 6 or " " in ticker or ticker == "UNKNOWN":
            return "UNKNOWN", None

        return ticker, reason

    except Exception as e:
        print(f"Error in find_ticker_from_web: {e}")
        return "UNKNOWN", None
