import os, re, json
from pathlib import Path
import fitz  # pymupdf
import pandas as pd
from tqdm import tqdm

def clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_pdfs(raw_dir="data/raw", out_path="data/processed/pages.parquet"):
    raw_dir = Path(raw_dir)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    pdfs = list(raw_dir.rglob("*.pdf"))
    for pdf_path in tqdm(pdfs, desc="Parsing PDFs"):
        company = pdf_path.parent.name
        doc_name = pdf_path.name
        doc = fitz.open(pdf_path)
        for i in range(len(doc)):
            page = doc[i]
            text = clean_text(page.get_text("text"))
            if len(text) < 30:
                continue
            rows.append({
                "company": company,
                "source_file": doc_name,
                "page": i + 1,
                "text": text
            })

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df)} pages -> {out_path}")

if __name__ == "__main__":
    parse_pdfs()