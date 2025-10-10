# -*- coding: utf-8 -*-
"""
Script para extraer el texto completo de noticias financieras
Entrada: PRUEBAAPINUEVA/news_finance_en.csv
Salida:  PRUEBAAPINUEVA/news_finance_full.csv
"""

import os, time, re, sys, requests, pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

import trafilatura
from readability import Document as ReadDoc
try:
    from newspaper import Article as NPArticle
    NEWSPAPER_OK = True
except Exception:
    NEWSPAPER_OK = False

# --- Paths ---
BASE_DIR = r"C:\Users\mpsua\OneDrive\Escritorio\ud\CUARTO\Primer_Cuatri\PLN\pruebaProyecto\Bloomberg-scraper\PRUEBAAPINUEVA"
INPUT_FILE = os.path.join(BASE_DIR, "news_finance_en.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "news_finance_full.csv")

# --- Configuración general ---
DEFAULT_TIMEOUT = 20
SLEEP_BETWEEN = 0.7
HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "en,en-US;q=0.9",
}

BLACKLIST_DOMAINS = {
    "consent.yahoo.com", "guce.yahoo.com", "login.yahoo.com",
    "consent.google.com",
}
CONSENT_PATTERNS = [
    r"Yahoo is part of the Yahoo family of brands",
    r"Will be right back",
    r"enable JavaScript and cookies",
    r"before continuing",
]

# --- Funciones de soporte ---
def mk_session():
    sess = requests.Session()
    retries = Retry(
        total=3, connect=3, read=3, backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS_BASE.copy())
    return sess

def is_blacklisted(url):
    try:
        d = urlparse(url).netloc.lower()
    except Exception:
        return False
    return any(bad in d for bad in BLACKLIST_DOMAINS)

def looks_like_consent(html):
    if not html:
        return False
    for pat in CONSENT_PATTERNS:
        if re.search(pat, html[:20000], re.I):
            return True
    return False

def fetch_html(session, url, timeout=DEFAULT_TIMEOUT):
    try:
        if is_blacklisted(url):
            return (0, url, None, "blacklisted_domain")
        r = session.get(url, timeout=timeout, allow_redirects=True)
        html = r.text
        if r.status_code != 200:
            return (r.status_code, r.url, html, f"http_status_{r.status_code}")
        if looks_like_consent(html):
            return (r.status_code, r.url, html, "consent_or_block_detected")
        return (r.status_code, r.url, html, None)
    except Exception as e:
        return (0, url, None, f"fetch_error:{e.__class__.__name__}:{e}")

def extract_trafilatura(html, url):
    try:
        downloaded = html if html else trafilatura.fetch_url(url)
        return trafilatura.extract(downloaded, include_comments=False, include_tables=False)
    except Exception:
        return None

def extract_readability(html):
    try:
        doc = ReadDoc(html)
        soup = BeautifulSoup(doc.summary(html_partial=True), "lxml")
        for bad in soup(["script", "style", "aside", "nav"]): bad.extract()
        txt = soup.get_text("\n", strip=True)
        return txt if txt and len(txt) > 100 else None
    except Exception:
        return None

def extract_newspaper(url):
    if not NEWSPAPER_OK:
        return None
    try:
        art = NPArticle(url)
        art.download(); art.parse()
        return art.text if len(art.text) > 100 else None
    except Exception:
        return None

def light_clean(text):
    if not text: return None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    seen, out = set(), []
    for ln in lines:
        low = ln.lower()
        if low in seen: continue
        seen.add(low); out.append(ln)
    txt = "\n".join(out)
    return re.sub(r"\n{3,}", "\n\n", txt).strip()

def extract_best(session, url):
    status, final_url, html, err = fetch_html(session, url)
    if status and status != 200:
        txt = extract_trafilatura(None, final_url)
        return txt, "trafilatura(fetch_url)", status, err, final_url
    # 1) trafilatura
    txt = extract_trafilatura(html, final_url)
    if txt and len(txt) > 200:
        return txt, "trafilatura", status, err, final_url
    # 2) readability
    txt = extract_readability(html)
    if txt and len(txt) > 200:
        return txt, "readability", status, err, final_url
    # 3) newspaper
    txt = extract_newspaper(final_url)
    if txt and len(txt) > 200:
        return txt, "newspaper3k", status, err, final_url
    # fallback fetch_url directo
    txt = extract_trafilatura(None, final_url)
    if txt and len(txt) > 200:
        return txt, "trafilatura(fetch_url_2)", status, err, final_url
    return None, None, status, err, final_url

# --- MAIN ---
def main():
    print(f"📂 Leyendo: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    session = mk_session()

    df["full_text"] = None
    df["extractor_used"] = None
    df["status"] = None
    df["error"] = None
    df["final_url"] = None
    df["text_length"] = 0

    for i, row in enumerate(df.itertuples(index=False), start=1):
        url = getattr(row, "url_original", None)
        if not isinstance(url, str) or not url.startswith("http"):
            df.loc[i-1, "error"] = "invalid_url"
            continue
        if is_blacklisted(url):
            df.loc[i-1, "error"] = "blacklisted_domain"
            continue

        txt, extractor, status, err, final_url = extract_best(session, url)
        clean_txt = light_clean(txt)
        df.loc[i-1, "full_text"] = clean_txt
        df.loc[i-1, "extractor_used"] = extractor
        df.loc[i-1, "status"] = status
        df.loc[i-1, "error"] = err
        df.loc[i-1, "final_url"] = final_url
        df.loc[i-1, "text_length"] = len(clean_txt) if clean_txt else 0

        if i % 20 == 0:
            print(f"[{i}/{len(df)}] {urlparse(url).netloc} → len={len(clean_txt) if clean_txt else 0}")
        time.sleep(SLEEP_BETWEEN)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Guardado: {OUTPUT_FILE}")
    print(df["extractor_used"].value_counts(dropna=False))
    print(df["error"].value_counts(dropna=False).head(10))

if __name__ == "__main__":
    main()

