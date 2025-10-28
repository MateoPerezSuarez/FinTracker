# 03_resolver_url_original.py
import os, time, glob, pathlib, re
import pandas as pd
import requests
from bs4 import BeautifulSoup
import tldextract

# ========= CONFIG =========
INPUT_GLOB = "data_processing/finnhubAPI/data/porEmpresas/urlsFinales/TSLA.csv" 
URL_COLS_CANDIDATAS = ["url_redirect", "url", "link"]
TIMEOUT = 25
SLEEP_BETWEEN = 0.1
HEADERS = {"User-Agent": "Mozilla/5.0 (resolver-url-original; +dataset)"}
# =========================

def pick_url_column(df):
    for c in URL_COLS_CANDIDATAS:
        if c in df.columns:
            return c
    # último intento: la primera columna que parezca URL
    for c in df.columns:
        if df[c].astype(str).str.startswith("http").any():
            return c
    raise ValueError(f"No encuentro columna de URL. Busca alguna de {URL_COLS_CANDIDATAS} o añade una que empiece por http")

def fetch_final_url(u: str):
    """Sigue redirecciones y devuelve (final_url, status_code, text_or_none).
    No levanta excepción salvo requests graves; devuelve (None, code, None) si falla.
    """
    try:
        r = requests.get(u, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        # algunos sites devuelven 200 pero con bloques de consent
        text = r.text if (r.status_code >= 200 and r.status_code < 400) else None
        return r.url, r.status_code, text
    except requests.RequestException as e:
        return None, None, str(e)

def extract_canonical(html: str, base_url: str):
    """Intenta sacar <link rel='canonical'> o <meta property='og:url'>.
    Devuelve URL absoluta si es posible; si no, None.
    """
    if not html: 
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
        # 1) rel=canonical
        link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
        if link and link.get("href"):
            return absolutize(link.get("href"), base_url)
        # 2) og:url
        og = soup.find("meta", property="og:url")
        if og and og.get("content"):
            return absolutize(og.get("content"), base_url)
    except Exception:
        pass
    return None

def absolutize(href: str, base: str):
    # resuelve URLs relativas simples
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # relativa sin protocolo
    try:
        from urllib.parse import urljoin
        return urljoin(base, href)
    except Exception:
        return None

def domain_of(u: str):
    try:
        ext = tldextract.extract(u)
        return ".".join([ext.domain, ext.suffix]) if ext.suffix else ext.domain
    except Exception:
        return ""

def choose_original(final_url: str, canon_url: str):
    """Heurística: preferimos canonical si parece válida y no es una ruta rara/trackeada."""
    def looks_tracking(u: str):
        return bool(re.search(r"(utm_|fbclid=|ref=|rss|feedproxy|news.google|amp/?)", (u or ""), re.I))
    if canon_url and canon_url.startswith("http") and not looks_tracking(canon_url):
        return canon_url
    return final_url

def process_file(path: str):
    df = pd.read_csv(path)
    url_col = pick_url_column(df)

    results = []
    for i, u in enumerate(df[url_col].astype(str).fillna("")):
        u = u.strip()
        if not u or not u.startswith("http"):
            results.append({"url_final": "", "url_canonical": "", "url_original": "", "domain": "", "http_status": None, "error": "no_url"})
            continue

        final_url, status, html_or_err = fetch_final_url(u)
        if isinstance(html_or_err, str) and final_url is None:
            # error de requests
            results.append({"url_final": "", "url_canonical": "", "url_original": "", "domain": "", "http_status": status, "error": html_or_err[:200]})
            time.sleep(SLEEP_BETWEEN); 
            continue

        html = html_or_err if isinstance(html_or_err, str) or html_or_err is None else None  # solo por tipado
        canon = extract_canonical(html_or_err if isinstance(html_or_err, str) else "", final_url or u)
        original = choose_original(final_url, canon)
        results.append({
            "url_final": final_url or "",
            "url_canonical": canon or "",
            "url_original": original or "",
            "domain": domain_of(original or final_url or u),
            "http_status": status,
            "error": "" if final_url else "fetch_failed",
        })
        time.sleep(SLEEP_BETWEEN)

    out = pd.concat([df, pd.DataFrame(results)], axis=1)
    out_path = pathlib.Path(path).with_name(pathlib.Path(path).stem + "_orig.csv")
    out.to_csv(out_path, index=False)
    return str(out_path), len(out)

def main():
    files = sorted(glob.glob(INPUT_GLOB))
    if not files:
        print(f"No encontré ficheros con patrón {INPUT_GLOB}")
        return
    print(f"Procesando {len(files)} ficheros…")
    total = 0
    for f in files:
        out_path, n = process_file(f)
        print(f"✓ {f} → {out_path} ({n} filas)")
        total += n
    print(f"Terminado. Filas totales procesadas: {total}")

if __name__ == "__main__":
    main()
