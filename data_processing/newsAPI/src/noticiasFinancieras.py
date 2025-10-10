# -*- coding: utf-8 -*-
# Ejecuta: python descargar_news_financieras_en.py
import os, time, json, requests
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

API_KEY = os.getenv("NEWSAPI_KEY")
assert API_KEY, "Falta NEWSAPI_KEY en tu entorno Conda (usa: conda env config vars set NEWSAPI_KEY=TU_CLAVE)."

BASE = "https://newsapi.org/v2/everything"

# --- CONFIGURACIÓN ---
IDIOMA = "en"
DIAS_ATRAS = 30
HOY_UTC = datetime.utcnow().date()
DESDE = HOY_UTC - timedelta(days=DIAS_ATRAS)

# --- CONSULTA FINANCIERA ---
QUERY = (
    '(bankruptcy OR insolvency OR "files for bankruptcy" OR "chapter 11" '
    'OR "stock plunges" OR "shares fall" OR "stock drops" OR "shares drop" '
    'OR downgrade OR "profit warning" OR layoffs OR restructuring '
    'OR merger OR acquisition OR takeover OR IPO OR "initial public offering" '
    'OR "earnings report" OR "quarterly results" OR "financial results" '
    'OR "appoints CEO" OR "resigns CEO" OR "chief executive" OR "steps down" '
    'OR "restructuring plan" OR "cut jobs" OR "mass layoffs")'
)

# --- FUENTES FINANCIERAS (principales medios económicos globales) ---
DOMAINS = (
    "bloomberg.com,reuters.com,ft.com,wsj.com,cnbc.com,marketwatch.com,"
    "businessinsider.com,forbes.com,seekingalpha.com,barrons.com,"
    "investing.com,finance.yahoo.com"
)

EXCLUDE = "sports.yahoo.com,autos.yahoo.com"

headers = {"X-Api-Key": API_KEY}

# --- FUNCIÓN PARA PEDIR UNA PÁGINA ---
def fetch(params):
    r = requests.get(BASE, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        try:
            print("Error body:", r.json())
        except Exception:
            print("Error text:", r.text[:300])
        r.raise_for_status()
    return r.json()

# --- DESCARGA POR DÍAS (sin paginar) ---
rows = []
current = DESDE
print(f"📅 Descargando noticias financieras en inglés desde {DESDE} hasta {HOY_UTC}")

while current <= HOY_UTC:
    params = {
        "q": QUERY,
        "language": IDIOMA,
        "from": current.strftime("%Y-%m-%d"),
        "to": current.strftime("%Y-%m-%d"),
        "sortBy": "publishedAt",
        "pageSize": 100,
        "page": 1,
        "domains": DOMAINS,
        "excludeDomains": EXCLUDE,
    }
    data = fetch(params)
    arts = data.get("articles", [])
    for a in arts:
        rows.append({
            "source": (a.get("source") or {}).get("name"),
            "author": a.get("author"),
            "headline": a.get("title"),
            "summary": a.get("description"),
            "url_original": a.get("url"),
            "url_image": a.get("urlToImage"),
            "publishedAt": a.get("publishedAt"),
            "content_truncated": a.get("content"),
            "language": IDIOMA,
        })
    time.sleep(0.5)
    current += timedelta(days=1)

# --- GUARDADO ---
df = pd.DataFrame(rows)
if not df.empty:
    df = df.drop_duplicates(subset=["url_original"]).drop_duplicates(subset=["headline"])

df.to_csv("news_finance_en.csv", index=False)
with open("news_finance_en.jsonl", "w", encoding="utf-8") as f:
    for _, r in df.iterrows():
        f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")

print(f"✅ Filas guardadas: {len(df)}")
print("Ejemplos:")
print(df.head(5)[["source", "headline", "publishedAt", "url_original"]])
