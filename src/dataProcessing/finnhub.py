# pip install pandas python-dateutil tldextract
import os, time, pathlib, random, requests
import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

# ============ CONFIG ============
API = os.getenv("FINNHUB_KEY") or "d3m03tpr01qkjssdop9gd3m03tpr01qkjssdopa0"
TICKERS_FIJOS = ["AAPL","MSFT","TSLA","META","GOOGL","NVDA","AMZN"]

PER_TICKER_TARGET = 1000
WINDOW_DAYS = 7
MAX_LOOKBACK_DAYS = 365

# Aleatorios
EXCHANGES = ["US"]              
RANDOM_NUM_TICKERS = 100 
RANDOM_PER_TICKER_TARGET = 50
SEED = 42

OUT_DIR = pathlib.Path("data_processing/finnhubAPI/data/porEmpresas/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (dataset builder)"}

def _rget(url, params, retries=3, backoff=1.2):
    for i in range(retries):
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if r.status_code == 429:
            time.sleep(backoff * (i+1)); continue
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized: token inválido/no enviado")
        if r.ok:
            return r
        time.sleep(backoff * (i+1))
    r.raise_for_status()
    return r

def company_news(symbol: str, _from: str, to: str):
    r = _rget("https://finnhub.io/api/v1/company-news",
              {"symbol": symbol, "from": _from, "to": to, "token": API})
    return r.json() or []

def list_symbols(exchange: str):
    r = _rget("https://finnhub.io/api/v1/stock/symbol",
              {"exchange": exchange, "token": API})
    return r.json() or []

def iso_from_epoch(ts):
    return datetime.utcfromtimestamp(ts).isoformat() if isinstance(ts, (int,float)) else None

def dedupe_by_url(items):
    seen, out = set(), []
    for a in items:
        u = a.get("url")
        if not u or u in seen: 
            continue
        seen.add(u); out.append(a)
    return out

def collect_company(symbol: str, target: int):
    collected = []
    end_dt = datetime.combine(date.today(), datetime.min.time())
    start_limit = end_dt - timedelta(days=MAX_LOOKBACK_DAYS)
    while len(collected) < target and end_dt > start_limit:
        start_dt = end_dt - timedelta(days=WINDOW_DAYS)
        batch = company_news(symbol, _from=start_dt.strftime("%Y-%m-%d"), to=end_dt.strftime("%Y-%m-%d"))
        collected.extend(batch)
        end_dt = start_dt - timedelta(seconds=1)
        time.sleep(0.15)
    return dedupe_by_url(collected)[:target]

def rows_from_items(items, ticker=""):
    rows = []
    for a in items:
        rows.append({
            "provider": "finnhub",
            "ticker": ticker,
            "published_utc": iso_from_epoch(a.get("datetime")),
            "headline": a.get("headline"),
            "summary": a.get("summary"),
            "url_redirect": a.get("url"),
            "image_url": a.get("image"),
            "source": a.get("source"),
        })
    return rows

def save_csv(rows, path: pathlib.Path):
    df = pd.DataFrame(rows)
    if not df.empty:
        df.drop_duplicates(subset=["url_redirect"], inplace=True)
    df.to_csv(path, index=False)

def sample_random_tickers(exchanges, n, exclude=set()):
    random.seed(SEED)
    all_syms = []
    for ex in exchanges:
        syms = list_symbols(ex)
        syms = [s for s in syms if s.get("symbol") and s["symbol"].isupper() and len(s["symbol"]) <= 6]
        all_syms.extend([s["symbol"] for s in syms])
        time.sleep(0.2)
    pool = [s for s in set(all_syms) if s not in exclude]
    if len(pool) < n:
        n = len(pool)
    return random.sample(pool, n)

def main():
    if not API or API == "TU_API_KEY_AQUI":
        raise RuntimeError("Falta la API key. Define FINNHUB_KEY o pega tu token en API.")

    all_rows = []

    # 1) Un CSV por empresa fija
    for t in TICKERS_FIJOS:
        print(f"[Fijo] {t}: recolectando…")
        items = collect_company(t, PER_TICKER_TARGET)
        rows = rows_from_items(items, ticker=t)
        save_csv(rows, OUT_DIR / f"{t}.csv")
        print(f"   ✓ {t}: {len(rows)} artículos → {OUT_DIR / f'{t}.csv'}")
        all_rows.extend(rows)

    # 2) Aleatorios: muestrea tickers y descarga
    print("[Aleatorios] muestreando tickers…")
    rnd_tickers = sample_random_tickers(EXCHANGES, RANDOM_NUM_TICKERS, exclude=set(TICKERS_FIJOS))
    print(f"   ✓ {len(rnd_tickers)} tickers aleatorios")

    rnd_rows = []
    for i, t in enumerate(rnd_tickers, 1):
        print(f"   ({i}/{len(rnd_tickers)}) {t}: recolectando…")
        try:
            items = collect_company(t, RANDOM_PER_TICKER_TARGET)
            rnd_rows.extend(rows_from_items(items, ticker=t))
        except Exception as e:
            print(f"     ! {t}: {e}")
        time.sleep(0.2)

    save_csv(rnd_rows, OUT_DIR / "RANDOM.csv")
    print(f"   ✓ Aleatorio total: {len(rnd_rows)} artículos → {OUT_DIR / 'RANDOM.csv'}")

    # 3) Índice combinado de todos
    combined = pd.DataFrame(all_rows + rnd_rows)
    if not combined.empty:
        combined.drop_duplicates(subset=["url_redirect"], inplace=True)
    combined.to_csv(OUT_DIR / "INDEX_ALL.csv", index=False)
    print(f"Índice combinado: {len(combined)} filas → {OUT_DIR / 'INDEX_ALL.csv'}")

if __name__ == "__main__":
    main()
