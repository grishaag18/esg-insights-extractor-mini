# ESG Insights Extractor (TMT)

## Overview

This project is a prototype designed to automate the extraction of investment-relevant ESG signals from corporate disclosures in the Technology, Media, and Telecommunications (TMT) sector.

It ingests sustainability and financial reports, parses unstructured text, retrieves relevant excerpts, and uses a Large Language Model (LLM) to extract structured ESG signals. These signals are then aggregated into a simple scorecard for cross-company comparison.

The objective is to demonstrate how:

- Unstructured disclosures can be converted into structured insights  
- ESG factors can be linked to financial materiality  
- AI workflows can support research automation  
- Practical constraints (cost, compute, token limits) shape system design  

---

## Architecture

The pipeline consists of five stages:

1. **PDF Parsing**
2. **Text Chunking**
3. **Keyword-Based Retrieval**
4. **LLM-Based ESG Signal Extraction**
5. **Scorecard Construction**

Each stage produces intermediate outputs for auditability and modular debugging.

---


---

## Stage 1 – Parsing (`parse.py`)

**Purpose:** Convert raw PDFs into structured page-level text.

**Process:**
- Iterates through `data/raw/<COMPANY>/`
- Extracts text per page using PyMuPDF
- Stores:
  - company
  - source_file
  - page number
  - extracted text
- Saves output to `pages.parquet`

**Why page-level storage?**
- Preserves page numbers for auditability
- Enables evidence referencing in final signals
- Avoids repeated PDF parsing

---

## Stage 2 – Chunking (`chunk.py`)

**Purpose:** Split long page text into smaller segments suitable for LLM prompts.

**Configuration:**
- Chunk size: ~1200 characters
- Overlap: ~200 characters

**Why this size?**
- Large enough to preserve context
- Small enough to avoid prompt overflow
- Balances recall and computational stability

Output: `chunks.parquet`

---

## Stage 3 – Retrieval

The system retrieves a limited number of chunks per company using keyword-based scoring.

**Why keyword retrieval (instead of embeddings)?**
- Lightweight and fast
- No additional model dependencies
- Suitable for prototype constraints

**Limitation:**
- Can over-select dominant topics (e.g., climate disclosures)
- May miss subtle ESG signals outside keyword scope

---

## Stage 4 – ESG Signal Extraction (`extract.py`)

**Purpose:** Convert selected excerpts into structured ESG signals using a local LLM.

**Process:**
1. Retrieve top-N relevant chunks per company
2. Build structured prompt with strict JSON schema
3. Call local LLM via Ollama
4. Apply JSON repair logic
5. Save output to `outputs/signals/<COMPANY>.json`

---

## LLM Configuration

**Model:** Local LLM (e.g., `phi3:mini`) via Ollama

### Why use a Local LLM?

| API-based LLM | Local LLM |
|---------------|------------|
| Higher accuracy | No token cost |
| Better JSON reliability | No quota limits |
| Faster inference | Offline usage |
| Requires billing | More prompt engineering |

This prototype uses a local model to:
- Avoid API token constraints
- Enable iterative development
- Keep costs at zero

**Tradeoff:**
- Slower inference
- Requires stricter prompt control
- Occasional JSON formatting errors

---

## ESG Signal Schema

Each signal includes:

- `topic_id`
- `signal_type`
- `summary`
- `financial_channel`
- `severity`
- `time_horizon`
- `evidence` (quote + source + page)

This ensures:
- Structured comparison
- Traceability to original disclosures
- Explicit linkage to financial channels

---

## Stage 5 – Scorecard Construction (`scorecard.py`)

**Purpose:** Aggregate signals into a structured comparison dataset.

**Scoring Logic:**
- Base score: 3.0 (neutral)
- Risks/controversies → penalized based on severity
- Opportunities/commitments → modest positive adjustment
- Score bounded between 1 and 5

Output: `outputs/scorecards/esg_scorecard.csv`

This scoring method is heuristic and intended for demonstration only.

---

## Design Constraints & Tradeoffs

### 1. Limited Context per Company
To prevent local LLM timeouts:
- Only top-N chunks are selected
- Each chunk has a character cap

**Tradeoff:**  
Stability and speed vs lower recall.

---

### 2. Topic Imbalance
Keyword retrieval may skew toward climate-related disclosures.

Potential improvements:
- Topic-balanced retrieval
- Embedding-based search

---

### 3. Local LLM Variability
Local models may:
- Produce invalid JSON
- Over-summarize
- Miss nuanced signals

Mitigation:
- Strict schema prompts
- JSON repair fallback logic

---

### 4. Heuristic Scoring
The scorecard:
- Is rule-based
- Does not use market data
- Is not predictive

It demonstrates methodology, not investment performance.

---

## Why This Matters

Sustainability reports are often long, narrative, and difficult to analyze systematically.

This system demonstrates how:

- ESG disclosures can be parsed automatically  
- Financially relevant signals can be structured  
- Evidence can be preserved for auditability  
- Research workflows can be partially automated  

---

## How to Run

1. Place PDFs in: data/raw/<<COMPANY>>/
2. Parse: python src/parse.py
3. Chunk: python src/chunk.py
4. Extract: python src/extract.py
5. Build scorecard: python src/scorecard.py
