import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml


TOPICS_PATH = "configs/esg_topics.yaml"
SIGNALS_DIR = Path("outputs/signals")
OUT_CSV = Path("outputs/scorecards/esg_scorecard.csv")


def load_topics(path=TOPICS_PATH):
    if not Path(path).exists():
        return {}
    with open(path, "r") as f:
        topics = yaml.safe_load(f).get("topics", [])
    return {t["id"]: t for t in topics}


def normalize_signal(sig: dict) -> dict:
    """Handle small schema inconsistencies from local LLM output."""
    s = dict(sig)

    # time_horizon field sometimes misspelled
    if "time_horizon" not in s:
        if "timehorizon" in s:
            s["time_horizon"] = s.pop("timehorizon")
        elif "timeinhorizon" in s:
            s["time_horizon"] = s.pop("timeinhorizon")

    # Ensure expected keys exist
    s.setdefault("topic_id", "unknown")
    s.setdefault("signal_type", "risk")
    s.setdefault("summary", "")
    s.setdefault("financial_channel", [])
    s.setdefault("severity", 3)
    s.setdefault("time_horizon", "1-3y")
    s.setdefault("evidence", [])

    return s


def score_topic(signals: list[dict]) -> tuple[float, str, str]:
    """
    Transparent scoring:
    Start 3.0
    - Penalize risks/controversies based on severity
    + Reward opportunities/commitments/metrics modestly (scaled by severity)
    """
    if not signals:
        return 3.0, "No extracted signals for this topic (default neutral).", ""

    score = 3.0
    rationale_bits = []
    evidence_bits = []

    for s in signals[:6]:
        stype = str(s.get("signal_type", "risk")).lower()
        sev = float(s.get("severity", 3))

        if stype in ["risk", "controversy"]:
            score -= (sev - 3) * 0.45
        else:  # opportunity/metric/commitment
            score += (sev - 3) * 0.20

        summary = s.get("summary", "")
        rationale_bits.append(f"{stype}: {summary}".strip())

        ev = s.get("evidence", [])
        if ev and isinstance(ev, list):
            e0 = ev[0]
            quote = str(e0.get("quote", ""))[:180]
            src = str(e0.get("source_file", ""))
            page = e0.get("page", "")
            if quote and src:
                evidence_bits.append(f"\"{quote}\" ({src} p{page})")

    score = max(1.0, min(5.0, round(score, 1)))
    return score, " | ".join(rationale_bits)[:900], " ; ".join(evidence_bits)[:900]


def build_scorecard():
    topics_map = load_topics()

    rows = []

    json_files = sorted(SIGNALS_DIR.glob("*.json"))
    json_files = [fp for fp in json_files if not fp.name.endswith("_raw.json")]

    for fp in json_files:
        data = json.loads(fp.read_text(encoding="utf-8"))

        company = data.get("company", fp.stem)
        raw_signals = data.get("signals", [])
        if not isinstance(raw_signals, list):
            raw_signals = []

        # normalize
        signals = [normalize_signal(s) for s in raw_signals if isinstance(s, dict)]

        # group by topic_id
        by_topic = {}
        for s in signals:
            by_topic.setdefault(s["topic_id"], []).append(s)

        # If you want a fixed set of topics, use configs; otherwise score whatever appears
        topic_ids = sorted(by_topic.keys()) if by_topic else []

        for topic_id in topic_ids:
            score, rationale, evidence = score_topic(by_topic[topic_id])
            topic_name = topics_map.get(topic_id, {}).get("name", topic_id)

            rows.append({
                "company": company,
                "topic_id": topic_id,
                "topic_name": topic_name,
                "score": score,
                "rationale": rationale,
                "key_evidence": evidence,
                "last_updated": datetime.utcnow().isoformat()
            })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    out = pd.DataFrame(rows)
    if out.empty:
        print("No rows produced. Check that outputs/signals/*.json contain 'signals' with topic_id.")
        out.to_csv(OUT_CSV, index=False)
        return

    out = out.sort_values(["company", "topic_id"])
    out.to_csv(OUT_CSV, index=False)
    print(f"Saved scorecard -> {OUT_CSV} ({len(out)} rows)")


if __name__ == "__main__":
    build_scorecard()