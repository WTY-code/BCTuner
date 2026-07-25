from langchain_openai import ChatOpenAI

from core.llm_config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL
from core.tools import diagnose, reflect, retrieve_knowledge


def make_llm() -> ChatOpenAI:
    llm = ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0.8,
        max_tokens=16384,
        timeout=600,
        max_retries=6,
    )
    return llm.bind_tools(
        [diagnose, retrieve_knowledge, reflect],
        tool_choice="auto",
    )
