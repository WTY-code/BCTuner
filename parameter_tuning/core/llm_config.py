"""LLM connection constants — read from environment variables.

Import from here in any module that needs LLM credentials.
This module has NO imports from the rest of the project,
preventing circular-import issues.

Set variables in your shell or in a .env file (see .env.example):
    DEEPSEEK_API_KEY   — your DeepSeek API key
    DEEPSEEK_BASE_URL  — API base URL (default: https://api.deepseek.com)
    DEEPSEEK_MODEL     — model name (default: deepseek-chat)
"""

import os

LLM_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
LLM_API_KEY  = os.getenv("DEEPSEEK_API_KEY",  "sk-124ed7d2d3f94c18947b8e3a3a8c6515")
LLM_MODEL    = os.getenv("DEEPSEEK_MODEL",     "deepseek-v4-pro")
