from pathlib import Path
import pandas as pd

def chunk_text(text: str, chunk_size=1200, overlap=200):
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + chunk_size, n)
        chunks.append(text[start:end])
        if end == n:
            break
        start = max(0, end - overlap)
    return chunks

def make_chunks(pages_path="data/processed/pages.parquet", out_path="data/processed/chunks.parquet"):
    pages_path = Path(pages_path)
    out_path = Path(out_path)
    df = pd.read_parquet(pages_path)

    rows = []
    chunk_id = 0
    for r in df.itertuples(index=False):
        for c in chunk_text(r.text):
            rows.append({
                "company": r.company,
                "source_file": r.source_file,
                "page": r.page,
                "chunk_id": chunk_id,
                "chunk_text": c
            })
            chunk_id += 1

    out = pd.DataFrame(rows)
    out.to_parquet(out_path, index=False)
    print(f"Saved {len(out)} chunks -> {out_path}")

if __name__ == "__main__":
    make_chunks()