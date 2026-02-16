"""
SEC 13F table parser using LLM for structured extraction.
Supports OpenAI (Agent/CRunner) or Gemini (Google API) via env:
  LLM_PARSER_MODEL=openai | gemini  (default: openai)
  GEMINI_API_KEY=...                (required when using gemini)
  GEMINI_MODEL=gemini-1.5-flash      (optional, default model name)
"""

import os
import json
from typing import List

from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

# Provider: "openai" (default) or "gemini"
LLM_PARSER_MODEL = os.getenv("LLM_PARSER_MODEL", "openai").strip().lower()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash").strip()

extract_system = """
You are an expert SEC 13F table parser.
Your task is to extract structured data from raw text tables of 13F filings.
The input will be a block of text representing one or more rows of a 13F filing table.
You must output a JSON object for each row, strictly following the schema below:

```json
{
    "name_of_issuer": string,
    "title_of_class": string,
    "cusip": string,
    "figi": string,
    "value": string,
    "shares_or_percent_amount": string,
    "shares_or_percent_type": string,
    "put_call": string,
    "investment_discretion": string,
    "other_manager": string,
    "voting_authority_sole": string,
    "voting_authority_shared": string,
    "voting_authority_none": string
}
```

### Rules:
* Always return valid JSON (an array if multiple rows).
* Numeric fields must contain only digits and optional commas; if missing or invalid use "0".
* String fields: if missing or invalid use "".
* Do not infer or fabricate information beyond what is in the table.
"""


class Sec13FEntry(BaseModel):
    name_of_issuer: str = Field("", description="Full company/issuer name")
    title_of_class: str = Field("", description="Security class")
    cusip: str = Field("", description="9-character CUSIP if available")
    figi: str = Field("", description="FIGI, leave empty if unavailable")
    value: str = Field("0", description="Numeric value as string")
    shares_or_percent_amount: str = Field("0", description="Shares/amount as string")
    shares_or_percent_type: str = Field("", description="Type e.g. SH, PRN")
    put_call: str = Field("", description="Put or Call")
    investment_discretion: str = Field("", description="e.g. Sole, Defined")
    other_manager: str = Field("", description="Other manager reference")
    voting_authority_sole: str = Field("0", description="Sole voting amount")
    voting_authority_shared: str = Field("0", description="Shared voting amount")
    voting_authority_none: str = Field("0", description="None voting amount")


class TableRes(BaseModel):
    result: List[Sec13FEntry] = Field(default_factory=list, description="List of records")


# OpenAI agent (used when LLM_PARSER_MODEL=openai)
extract_agent = None


def _get_openai_agent():
    global extract_agent
    if extract_agent is None:
        from agents import Agent
        extract_agent = Agent(
            name="Extract Agent",
            instructions=extract_system,
            model="gpt-5-nano",
        )
    return extract_agent


def _normalize_entries(raw):
    """Convert parser output to list of dicts with expected keys."""
    if not raw:
        return []
    return [
        e if isinstance(e, dict) else (e.model_dump() if hasattr(e, "model_dump") else e)
        for e in raw
    ]


def _gemini_generation_config(genai):
    """Build GenerationConfig with structured output schema (TableRes)."""
    schema = TableRes.model_json_schema()
    for key in ("response_schema", "response_json_schema"):
        try:
            return genai.GenerationConfig(
                response_mime_type="application/json",
                temperature=0.1,
                **{key: schema},
            )
        except (TypeError, AttributeError):
            continue
    return genai.GenerationConfig(
        response_mime_type="application/json",
        temperature=0.1,
    )


async def _get_records_gemini(table_str: str) -> list:
    """Use Google Gemini API structured output for table extraction. Returns list of dicts."""
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY is not set; required when LLM_PARSER_MODEL=gemini")
    try:
        import google.generativeai as genai
    except ImportError:
        raise ImportError("Install google-generativeai: pip install google-generativeai")

    genai.configure(api_key=GEMINI_API_KEY)
    config = _gemini_generation_config(genai)
    model = genai.GenerativeModel(
        GEMINI_MODEL,
        system_instruction=extract_system,
        generation_config=config,
    )
    prompt = f"Parse this text table into structured rows.\n\nTable text:\n{table_str}"
    response = model.generate_content(prompt)
    text = (response.text or "").strip()
    if not text:
        return []
    try:
        parsed = TableRes.model_validate_json(text)
        return _normalize_entries(parsed.result)
    except Exception:
        # Fallback: try raw JSON parse if schema validation fails
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "result" in data:
                return _normalize_entries(data["result"])
            if isinstance(data, list):
                return _normalize_entries(data)
        except json.JSONDecodeError:
            pass
        return []


async def get_records(table_str):
    """Parse table text into a list of Sec13FEntry-like dicts. Uses OpenAI or Gemini per env."""
    if LLM_PARSER_MODEL == "gemini":
        return await _get_records_gemini(table_str)

    # OpenAI path
    from utils.custom_runners import CRunner
    agent = _get_openai_agent()
    prompt = f"Parse this text table into json list.\nTable text : {table_str}"
    res_obj = CRunner(
        agent=agent,
        prompt=prompt,
        format_output=TableRes,
    )
    await res_obj.run_async()
    res = res_obj.output
    if not isinstance(res, dict):
        return []
    raw = res.get("result", [])
    return _normalize_entries(raw)
