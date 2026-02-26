import json
import re
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# -----------------------------
# Config
# -----------------------------
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "phi3:mini"  # safest on MacBook Air
# Alternatives (if you have them pulled):
# OLLAMA_MODEL = "llama3.1:8b-instruct-q4_0"
# OLLAMA_MODEL = "mistral:7b-instruct-q4_0"

CHUNKS_PATH = "data/processed/chunks.parquet"
OUT_DIR = Path("outputs/signals")

TOP_CHUNKS = 8          # keep small for local models
CHUNK_CHAR_CAP = 700    # cap each chunk text
MAX_SIGNALS = 12        # keep output concise


# -----------------------------
# Helpers
# -----------------------------
def call_ollama(prompt: str, model: str = OLLAMA_MODEL) -> str:
    """Call local Ollama model and return text response."""
    resp = requests.post(
        OLLAMA_URL,
        json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            # lower temp = more structured JSON
            "options": {"temperature": 0.2},
        },
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json().get("response", "")


def extract_json_from_text(text: str):
    # Remove markdown fences
    text = text.replace("```json", "").replace("```", "").strip()

    # Try direct parse
    try:
        return json.loads(text)
    except:
        pass

    # Extract first JSON block
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1:
        candidate = text[start:end+1]
        try:
            return json.loads(candidate)
        except:
            return None

    return None

def build_prompt(company: str, excerpts: str) -> str:
    return f"""
You are an investment research assistant.

TASK:
Extract financially material ESG signals from the EXCERPTS for company {company}.

Financially material means the issue can affect:
- revenue
- costs
- capex
- regulatory exposure
- reputation
- supply constraints
- cost of capital

STRICT RULES:
- Return ONLY valid JSON.
- Do NOT include markdown.
- Do NOT include backticks.
- Do NOT include explanations.
- Do NOT write anything before or after JSON.
- Do NOT use "..." anywhere.
- If no real signals are found, return:
  {{"company":"{company}","signals":[]}}
- Otherwise extract between 3 and 8 real signals.

Allowed topic_id values:
- climate_energy
- privacy_security
- ai_regulation
- supply_chain
- legal_antitrust
- disclosure_quality

Allowed signal_type values:
- risk
- opportunity
- metric
- commitment
- controversy

Allowed financial_channel values:
- revenue
- cost
- capex
- regulatory
- reputation
- supply
- cost_of_capital

Each signal must:
- Be based ONLY on the provided EXCERPTS
- Include a short direct quote in evidence
- Use the correct source_file and page number
- Assign severity from 1 (low) to 5 (high)

FORMAT EXACTLY:

{{
  "company": "{company}",
  "signals": [
    {{
      "topic_id": "climate_energy",
      "signal_type": "risk",
      "summary": "Clear 1-2 sentence description of the signal.",
      "financial_channel": ["regulatory"],
      "severity": 4,
      "time_horizon": "1-3y",
      "evidence": [
        {{
          "quote": "Short direct quote from excerpt",
          "source_file": "file.pdf",
          "page": 12
        }}
      ]
    }}
  ]
}}

EXCERPTS:
{excerpts}
""".strip()


def company_keywords_score(text: str) -> int:
    # Lightweight keyword scoring to pick ESG-heavy chunks
    kws = [
        "emission", "emissions", "scope", "carbon", "renewable", "energy", "climate", "data center",
        "privacy", "gdpr", "ccpa", "breach", "cyber", "security",
        "ai", "algorithm", "model", "governance", "responsible ai",
        "supplier", "supply chain", "labor", "human rights", "audit", "sourcing",
        "antitrust", "investigation", "lawsuit", "fine", "regulator",
        "assurance", "disclosure", "target", "metrics",
    ]
    t = text.lower()
    return sum(t.count(k) for k in kws)


def build_company_excerpts(df_company: pd.DataFrame) -> str:
    # score & take top chunks
    df = df_company.copy()
    df["kw_score"] = df["chunk_text"].map(company_keywords_score)
    df = df[df["kw_score"] > 0].sort_values("kw_score", ascending=False).head(TOP_CHUNKS)

    excerpts = []
    for r in df.itertuples(index=False):
        short = r.chunk_text[:CHUNK_CHAR_CAP]
        excerpts.append(f"[{r.source_file} p{r.page}] {short}")
    return "\n\n".join(excerpts)


# -----------------------------
# Main extraction
# -----------------------------
def extract_per_company():
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    chunks = pd.read_parquet(CHUNKS_PATH)
    companies = sorted(chunks["company"].unique())

    for company in tqdm(companies, desc="Extracting per company (local LLM)"):
        dfc = chunks[chunks["company"] == company]
        excerpts = build_company_excerpts(dfc)

        raw_path = OUT_DIR / f"{company}_raw.txt"
        json_path = OUT_DIR / f"{company}.json"

        if not excerpts.strip():
            # nothing retrieved
            data = {"company": company, "signals": []}
            json_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            continue

        prompt = build_prompt(company, excerpts)

        # Call Ollama
        try:
            response_text = call_ollama(prompt)
        except Exception as e:
            raw_path.write_text(f"ERROR calling Ollama: {e}", encoding="utf-8")
            continue

        raw_path.write_text(response_text, encoding="utf-8")

        data = extract_json_from_text(response_text)
        if data is None:
            # keep raw; mark invalid
            (OUT_DIR / f"{company}_invalid.txt").write_text(response_text, encoding="utf-8")
            continue

        # Basic sanity checks
        if "company" not in data:
            data["company"] = company
        if "signals" not in data or not isinstance(data["signals"], list):
            data["signals"] = []

        json_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


if __name__ == "__main__":
    extract_per_company()