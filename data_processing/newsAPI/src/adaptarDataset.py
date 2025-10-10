import pandas as pd
from urllib.parse import urlparse
from datetime import datetime
import re
import math

INPUT_CSV  = r"PRUEBAAPINUEVA/datas/news_finance_full.csv"
OUTPUT_CSV = r"PRUEBAAPINUEVA/datas/news_finance_formatted.csv"
DEFAULT_CATEGORY = "finance"

def to_unix(ts):
    if pd.isna(ts):
        return ""
    s = str(ts).strip()
    try:
        if s.endswith("Z"):
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return int(datetime.strptime(s[:len(fmt)], fmt).timestamp())
            except Exception:
                pass
        return ""

def normalized_domain(u):
    if not isinstance(u, str) or not u:
        return ""
    try:
        netloc = urlparse(u).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""

def derive_http_code(row):
    # usamos 'error' y 'status' del INPUT si sirven, pero no las incluimos en el OUTPUT
    err = row.get("error", None)
    status = row.get("status", None)
    full_text = row.get("full_text", "")
    try:
        if isinstance(err, (int, float)) and not math.isnan(err):
            return int(err)
        if isinstance(err, str):
            m = re.search(r"\b(\d{3})\b", err)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    try:
        if isinstance(status, (int, float)) and not math.isnan(status):
            v = int(status)
            if 100 <= v <= 599:
                return v
    except Exception:
        pass
    return 200 if isinstance(full_text, str) and full_text.strip() else 0

df = pd.read_csv(INPUT_CSV)
out = pd.DataFrame()

out["category"] = pd.Series([str(DEFAULT_CATEGORY)] * len(df), dtype="string")

out["datetime"] = df.get("publishedAt", "").apply(to_unix)
out["headline"] = df.get("headline", "")
out["id"] = pd.RangeIndex(start=1, stop=len(df)+1, step=1)

out["image"] = df.get("url_image", "")
out["related"] = ""
out["source"] = df.get("source", "")
out["summary"] = df.get("summary", "")

out["url"] = df.get("final_url", "").fillna(df.get("url_original", ""))
out["url_original"] = df.get("url_original", "")

out["status"] = df.get("status", "")

out["http_code"] = df.apply(derive_http_code, axis=1)

out["target_url"] = df.get("final_url", "").fillna(df.get("url_original", ""))
out["final_url"]  = df.get("final_url", "").fillna(df.get("url_original", ""))

prefer_for_domain = out["final_url"].where(out["final_url"].astype(bool), out["url_original"])
out["domain"] = [normalized_domain(u) for u in prefer_for_domain]

out["http_status"] = out["http_code"]

out["article_text"] = df.get("full_text", "").fillna(df.get("content_truncated", ""))
out["word_count"] = out["article_text"].apply(
    lambda t: len([w for w in re.split(r"\s+", t.strip()) if w]) if isinstance(t, str) else 0
)

final_cols = [
    "category","datetime","headline","id","image","related","source","summary",
    "url","url_original","status","http_code","target_url","domain","final_url",
    "http_status","article_text","word_count"
]
out = out.reindex(columns=final_cols)

for c in ["category","headline","image","related","source","summary","url","url_original",
          "target_url","domain","final_url","article_text","status"]:
    out[c] = out[c].fillna("").astype("string")

out.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Listo: {OUTPUT_CSV}")



