# ============================================================
# Scraper con detección de cookie wall + fallback AMP
# - Carga robusta de CSV (encoding, líneas malas)
# - Usa 'url_original' si existe, si no cae a 'url'
# - Extracción: Trafilatura -> AMP -> Newspaper3k
# - Reintentos, concurrencia y checkpoints
# Ejecuta:  python src/pruebaScraping.py
# ============================================================

import asyncio, aiohttp, async_timeout, random, re, time, sys
from aiohttp import ClientTimeout
import pandas as pd
import trafilatura
from trafilatura.settings import use_config
from newspaper import Article
import tldextract
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin
from tqdm import tqdm

# --------- RUTAS (ajusta si hace falta) ---------
INPUT_CSV  = "data_processing/ticker_news_with_original.csv"
OUTPUT_CSV = "data_processing/ticker_news_with_text.csv"
OUTPUT_PARQUET = "data_processing/ticker_news_with_text.parquet"

# Si estás en WSL y el archivo está en OneDrive/Windows, puedes usar la ruta absoluta WSL:
# INPUT_CSV = r"/mnt/c/Users/tuusuario/OneDrive/.../ticker_news_with_original.csv"

# --------- PARÁMETROS ---------
CHECKPOINT_EVERY = 200       # guarda cada N filas
MIN_WORDS = 120              # umbral mínimo de calidad
GLOBAL_CONN_LIMIT = 16       # conexiones simultáneas
PER_DOMAIN_MIN_DELAY = 0.6   # segundos entre hits por dominio
REQUEST_TIMEOUT = 25         # timeout por request (s)

# (Opcional) listas de dominios
DOMAIN_BLACKLIST = {"bloomberg.com", "ft.com", "wsj.com"}  # paywalls duros
DOMAIN_WHITELIST = set()  # p.ej.: {"reuters.com","finance.yahoo.com"}

# --------- CARGA ROBUSTA DEL CSV ---------
def load_csv_robust(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path,
            engine="python",
            sep=",",
            quotechar='"',
            escapechar="\\",
            encoding="utf-8-sig",
            dtype=str,
            on_bad_lines="skip"
        )
    except Exception:
        return pd.read_csv(
            path,
            engine="python",
            sep=",",
            quotechar='"',
            escapechar="\\",
            encoding="latin-1",
            dtype=str,
            on_bad_lines="skip"
        )

df = load_csv_robust(INPUT_CSV)

# Detectar columna objetivo (preferimos url_original; si no existe, caemos a url)
if "url_original" in df.columns:
    target_col = "url_original"
elif "url" in df.columns:
    target_col = "url"  # menos ideal (endpoint), pero lo intentamos
else:
    raise ValueError("No encuentro ninguna columna de URLs ('url_original' ni 'url').")

# Filtrar filas con URL válida
df = df.copy()
df = df[df[target_col].notna() & (df[target_col].str.strip() != "")]
df["target_url"] = df[target_col].astype(str)

# Normalización de URL (quita trackers comunes)
def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if not (k.startswith("utm_") or k in {"fbclid", "gclid", "mc_cid", "mc_eid"})]
        p2 = p._replace(query=urlencode(q, doseq=True))
        return urlunparse(p2)
    except Exception:
        return u

df["target_url"] = df["target_url"].map(clean_url)

# Dominio
def domain_of(u: str) -> str:
    ext = tldextract.extract(u)
    return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain

df["domain"] = df["target_url"].map(domain_of)

# Filtrado opcional por listas
if DOMAIN_WHITELIST:
    df = df[df["domain"].isin(DOMAIN_WHITELIST)]
if DOMAIN_BLACKLIST:
    df = df[~df["domain"].isin(DOMAIN_BLACKLIST)]

# Deduplicar
df = df.drop_duplicates(subset=["target_url"]).reset_index(drop=True)

# Añadir columnas de salida si faltan
for col in ["final_url","http_status","article_text","word_count","extractor","error"]:
    if col not in df.columns:
        df[col] = None

# --------- CONFIG TRAFILATURA ---------
cfg = use_config()
cfg.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
cfg.set("DEFAULT", "MIN_EXTRACTED_SIZE", "200")
cfg.set("DEFAULT", "EXTRACTION_TECHNIQUE", "fast")

# --------- HEADERS / RATE LIMIT ---------
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]
def headers_base():
    return {
        "User-Agent": random.choice(UA_LIST),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7,es;q=0.6",
        "Connection": "keep-alive",
    }

last_hit = {}
async def rate_limit(url):
    d = domain_of(url)
    now = time.time()
    prev = last_hit.get(d, 0)
    wait = max(0, PER_DOMAIN_MIN_DELAY - (now - prev))
    if wait > 0:
        await asyncio.sleep(wait)
    last_hit[d] = time.time()

# --------- Heurística de “cookie wall” y AMP ---------
COOKIE_WALL_PATTERNS = [
    "we use cookies", "privacy policy", "cookie policy", "manage privacy",
    "iab transparency & consent framework", "consent", "reject all", "accept all",
    "cookie settings", "privacy dashboard"
]

def looks_like_cookie_wall(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(pat in t for pat in COOKIE_WALL_PATTERNS)

def find_amp_link(html: str, base_url: str) -> str | None:
    # Busca <link rel="amphtml" href="...">
    m = re.search(r'<link[^>]+rel=["\']amphtml["\'][^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urljoin(base_url, m.group(1))
    # Heurística: probar "/amp" si no hay link explícito
    try:
        if base_url.endswith("/"):
            return base_url + "amp"
        else:
            return base_url.rstrip("/") + "/amp"
    except Exception:
        return None

# Forzar preferencia AMP en dominios conocidos (p.ej. Yahoo)
YAHOO_DOMAINS = {"finance.yahoo.com", "uk.finance.yahoo.com", "news.yahoo.com"}
def prefer_amp_for(url: str) -> str | None:
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if host in YAHOO_DOMAINS:
        # muchos artículos tienen /amphtml/ o /amp
        # probamos añadir '/amphtml' manteniendo path
        amp_candidate = urljoin(url, "/".join(["/amphtml"] + [seg for seg in p.path.split("/") if seg]))
        return amp_candidate
    return None

# --------- NETWORK (reintentos) ---------
async def fetch_html(session, url, max_retries=3):
    await rate_limit(url)
    backoff = 0.8
    for attempt in range(1, max_retries + 1):
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, headers=headers_base(), allow_redirects=True) as r:
                    final_url = str(r.url)
                    status = r.status
                    # reintenta en throttling/errores 5xx
                    if status in (429,) or 500 <= status < 600:
                        raise aiohttp.ClientResponseError(
                            request_info=r.request_info, history=r.history, status=status, message="retryable"
                        )
                    if status >= 400:
                        return final_url, status, None
                    html = await r.text(errors="ignore")
                    return final_url, status, html
        except Exception as e:
            if attempt == max_retries:
                return None, None, f"ERROR_FETCH: {e}"
            await asyncio.sleep(backoff)
            backoff *= 1.8

def extract_with_trafilatura(html, url):
    return trafilatura.extract(html, url=url, config=cfg, include_comments=False)

def extract_with_newspaper(url):
    try:
        art = Article(url=url, keep_article_html=False)
        art.download()
        art.parse()
        text = art.text
        return text if text and len(text.split()) > 50 else None
    except Exception:
        return None

def clean_text(text: str) -> str:
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()

sem = asyncio.Semaphore(GLOBAL_CONN_LIMIT)

def good_text(t: str | None) -> bool:
    return bool(t) and len(t.split()) >= MIN_WORDS and not looks_like_cookie_wall(t or "")

async def process_row(session, i, row):
    orig_url = row["target_url"]

    # 0) Intento “prefer AMP” en dominios problemáticos (ej. Yahoo)
    amp_pref = prefer_amp_for(orig_url)
    tried_urls = []

    async def fetch_and_extract(u: str):
        async with sem:
            f_url, status, html = await fetch_html(session, u)
        if isinstance(html, str) and html.startswith("ERROR_FETCH:"):
            return f_url, status, None, "ERROR_FETCH"
        if not html:
            return f_url, status, None, "NO_HTML"
        text = extract_with_trafilatura(html, f_url or u)
        extractor = "trafilatura"
        if not good_text(text):
            # buscar amp si no era un URL AMP
            amp_url = find_amp_link(html, f_url or u)
            if amp_url and amp_url not in tried_urls:
                tried_urls.append(amp_url)
                async with sem:
                    f_amp, s_amp, h_amp = await fetch_html(session, amp_url)
                if h_amp:
                    amp_text = extract_with_trafilatura(h_amp, f_amp or amp_url) or extract_with_newspaper(f_amp or amp_url)
                    if good_text(amp_text):
                        return f_amp or f_url, s_amp, clean_text(amp_text), "trafilatura-amp"
        # fallback newspaper3k
        if not good_text(text):
            fallback = extract_with_newspaper(f_url or u)
            if good_text(fallback):
                return f_url, status, clean_text(fallback), "newspaper3k"
        return f_url, status, (clean_text(text) if text else None), extractor

    final_url, status, text, extractor = None, None, None, None

    # Prefer AMP primero si aplica
    if amp_pref:
        tried_urls.append(amp_pref)
        final_url, status, text, extractor = await fetch_and_extract(amp_pref)

    # Si no hay buen texto aún, intenta con la URL original
    if not good_text(text):
        tried_urls.append(orig_url)
        final_url, status, text, extractor = await fetch_and_extract(orig_url)

    ok = good_text(text)
    return i, {
        "final_url": final_url,
        "http_status": status,
        "article_text": text,
        "extractor": extractor if ok else (extractor or None),
        "error": None if ok else "Cookie wall or too short after AMP/fallback",
        "word_count": len(text.split()) if text else 0
    }

async def main():
    # Compat Windows
    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore
        except Exception:
            pass

    timeout = ClientTimeout(total=REQUEST_TIMEOUT + 10)
    conn = aiohttp.TCPConnector(limit=GLOBAL_CONN_LIMIT, ssl=False)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        tasks = [process_row(session, i, row) for i, row in df.iterrows()]
        done = 0
        for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping"):
            i, res = await fut
            for k, v in res.items():
                df.at[i, k] = v
            done += 1
            if done % CHECKPOINT_EVERY == 0:
                df.to_csv(OUTPUT_CSV, index=False)
                try:
                    df.to_parquet(OUTPUT_PARQUET, index=False)
                except Exception:
                    pass

    # Guardar final
    df.to_csv(OUTPUT_CSV, index=False)
    try:
        df.to_parquet(OUTPUT_PARQUET, index=False)
    except Exception:
        pass

if __name__ == "__main__":
    asyncio.run(main())
