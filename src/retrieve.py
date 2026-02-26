import re
import pandas as pd
import yaml
from pathlib import Path

def load_topics(path="configs/esg_topics.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)["topics"]

def keyword_retrieve(chunks_df: pd.DataFrame, topic, top_n=25):
    # simple scoring: count keyword hits
    kws = [k.lower() for k in topic["keywords"]]
    def score(txt):
        t = txt.lower()
        return sum(t.count(k) for k in kws)
    scored = chunks_df.copy()
    scored["kw_score"] = scored["chunk_text"].map(score)
    scored = scored[scored["kw_score"] > 0].sort_values("kw_score", ascending=False)
    return scored.head(top_n)

def build_topic_packets(chunks_path="data/processed/chunks.parquet", out_dir="data/processed/topic_packets"):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    chunks = pd.read_parquet(chunks_path)
    topics = load_topics()

    for topic in topics:
        packets = []
        for company, dfc in chunks.groupby("company"):
            top = keyword_retrieve(dfc, topic, top_n=25)
            packets.append(top.assign(topic_id=topic["id"]))
        out = pd.concat(packets, ignore_index=True) if packets else pd.DataFrame()
        out.to_parquet(out_dir / f"{topic['id']}.parquet", index=False)
        print(f"Saved {topic['id']} packets -> {len(out)} rows")

if __name__ == "__main__":
    build_topic_packets()