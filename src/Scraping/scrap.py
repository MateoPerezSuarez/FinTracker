# ============================================================
# Scraper de noticias con detección de cookie wall + AMP + Playwright
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
from playwright.async_api import async_playwright

# ======= RUTAS =======
INPUT_CSV  = "data_processing/ticker_news_with_original.csv"
OUTPUT_CSV = "data_processing/pruebas/ticker_news_with_text.csv"
OUTPUT_PARQUET = "data_processing/pruebas/ticker_news_with_text.parquet"
# Si estás en WSL y el archivo está en OneDrive/Windows, puedes usar la ruta absoluta WSL:
# INPUT_CSV = r"/mnt/c/Users/tuusuario/OneDrive/.../ticker_news_with_original.csv"

# ======= PARÁMETROS =======
CHECKPOINT_EVERY = 200
MIN_WORDS = 120
GLOBAL_CONN_LIMIT = 16
PER_DOMAIN_MIN_DELAY = 0.6
REQUEST_TIMEOUT = 25
DOMAIN_BLACKLIST = {"bloomberg.com", "ft.com", "wsj.com"}  # paywalls duros
DOMAIN_WHITELIST = set()  # p.ej.: {"reuters.com","finance.yahoo.com"}

# ======= CARGA ROBUSTA =======
def load_csv_robust(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path, engine="python", sep=",", quotechar='"', escapechar="\\",
            encoding="utf-8-sig", dtype=str, on_bad_lines="skip"
        )
    except Exception:
        return pd.read_csv(
            path, engine="python", sep=",", quotechar='"', escapechar="\\",
            encoding="latin-1", dtype=str, on_bad_lines="skip"
        )

df = load_csv_robust(INPUT_CSV)

# Detectar columna de URL (preferimos url_original)
if "url_original" in df.columns:
    target_col = "url_original"
elif "url" in df.columns:
    target_col = "url"  # menos ideal (endpoint), pero lo usamos si no hay url_original
else:
    raise ValueError("No se encuentra ninguna columna de URLs ('url_original' ni 'url').")

# Filtrado básico y normalización
df = df[df[target_col].notna() & (df[target_col].str.strip() != "")]
df["target_url"] = df[target_col].astype(str)

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

def domain_of(u: str) -> str:
    ext = tldextract.extract(u)
    return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain

df["domain"] = df["target_url"].map(domain_of)
if DOMAIN_WHITELIST:
    df = df[df["domain"].isin(DOMAIN_WHITELIST)]
if DOMAIN_BLACKLIST:
    df = df[~df["domain"].isin(DOMAIN_BLACKLIST)]
df = df.drop_duplicates(subset=["target_url"]).reset_index(drop=True)

for col in ["final_url","http_status","article_text","word_count","extractor","error"]:
    if col not in df.columns:
        df[col] = None

# ======= CONFIG TRAFILATURA =======
cfg = use_config()
cfg.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
cfg.set("DEFAULT", "MIN_EXTRACTED_SIZE", "200")
cfg.set("DEFAULT", "EXTRACTION_TECHNIQUE", "fast")

# ======= HEADERS / RATE LIMIT =======
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

# ======= Heurística de cookie wall + AMP =======
COOKIE_WALL_PATTERNS = [
    "we use cookies", "privacy policy", "cookie policy", "manage privacy",
    "iab transparency", "reject all", "accept all", "privacy dashboard"
]
def looks_like_cookie_wall(text: str | None) -> bool:
    if not text: return False
    t = text.lower()
    return any(p in t for p in COOKIE_WALL_PATTERNS)

def find_amp_link(html: str, base_url: str) -> str | None:
    m = re.search(r'<link[^>]+rel=["\']amphtml["\'][^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return urljoin(base_url, m.group(1))
    try:
        return (base_url + "amp") if base_url.endswith("/") else (base_url.rstrip("/") + "/amp")
    except Exception:
        return None

# ======= Playwright (renderizado + cerrar cookies) =======
class RenderExtractor:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.sem = asyncio.Semaphore(4)  # páginas simultáneas

    async def start(self):
        if self.playwright is None:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True, args=["--disable-gpu","--no-sandbox"]
            )

    async def stop(self):
        try:
            if self.browser: await self.browser.close()
            if self.playwright: await self.playwright.stop()
        except Exception:
            pass

    async def extract(self, url: str, timeout_ms: int = 12000) -> str | None:
        async with self.sem:
            ctx = await self.browser.new_context(ignore_https_errors=True, user_agent=random.choice(UA_LIST))
            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                # Intentos de cerrar CMP
                for sel in [
                    'button:has-text("Reject all")','button:has-text("Reject")',
                    'button:has-text("Rechazar todo")','button:has-text("Aceptar todo")',
                    'button[aria-label="Reject all"]','button[aria-label="Accept all"]',
                    'text="Manage privacy settings"'
                ]:
                    try:
                        loc = page.locator(sel).first
                        if await loc.count() > 0:
                            await loc.click(timeout=1500)
                            await page.wait_for_timeout(500)
                    except Exception:
                        continue
                await page.wait_for_timeout(800)
                html = await page.content()
            finally:
                await page.close(); await ctx.close()
        return html

# ======= Extracción =======
async def fetch_html(session, url, max_retries=3):
    await rate_limit(url)
    backoff = 0.8
    for attempt in range(1, max_retries + 1):
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, headers=headers_base(), allow_redirects=True) as r:
                    final_url = str(r.url); status = r.status
                    if status in (429,) or 500 <= status < 600:
                        raise aiohttp.ClientResponseError(
                            request_info=r.request_info, history=r.history, status=status, message="retryable"
                        )
                    if status >= 400:
                        return final_url, status, None
                    html = await r.text(errors="ignore")
                    return final_url, status, html
        except Exception:
            if attempt == max_retries:
                return None, None, None
            await asyncio.sleep(backoff); backoff *= 1.8

def extract_with_trafilatura(html, url):
    return trafilatura.extract(html, url=url, config=cfg, include_comments=False)

def extract_with_newspaper(url):
    try:
        art = Article(url=url, keep_article_html=False)
        art.download(); art.parse()
        t = art.text
        return t if t and len(t.split()) > 50 else None
    except Exception:
        return None

def clean_text(text: str) -> str:
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()

renderer = None
sem = asyncio.Semaphore(GLOBAL_CONN_LIMIT)

def good_text(t: str | None) -> bool:
    return bool(t) and len(t.split()) >= MIN_WORDS and not looks_like_cookie_wall(t)

async def process_row(session, i, row):
    url = row["target_url"]

    async with sem:
        final_url, status, html = await fetch_html(session, url)

    if not html:
        return i, {"error":"Empty HTML","http_status":status,"final_url":final_url,"article_text":None,"extractor":None,"word_count":0}

    # 1) Extracción normal
    text = extract_with_trafilatura(html, final_url or url)
    extractor = "trafilatura"

    # 2) AMP si el texto es malo o huele a cookies
    if not good_text(text):
        amp_url = find_amp_link(html, final_url or url)
        if amp_url:
            async with sem:
                amp_final, amp_status, amp_html = await fetch_html(session, amp_url)
            if amp_html:
                amp_text = extract_with_trafilatura(amp_html, amp_final or amp_url) or extract_with_newspaper(amp_final or amp_url)
                if good_text(amp_text):
                    text = clean_text(amp_text)
                    extractor = "trafilatura-amp"
                    final_url = amp_final or final_url

    # 3) Renderizado con Playwright si sigue mal
    if not good_text(text):
        try:
            rendered_html = await renderer.extract(final_url or url)
            if rendered_html:
                rend_text = extract_with_trafilatura(rendered_html, final_url or url) or extract_with_newspaper(final_url or url)
                if good_text(rend_text):
                    text = clean_text(rend_text)
                    extractor = "playwright+trafilatura"
        except Exception:
            pass

    # 4) Último fallback
    if not good_text(text):
        fb = extract_with_newspaper(final_url or url)
        if good_text(fb):
            text = clean_text(fb)
            extractor = "newspaper3k"

    text = clean_text(text or "")
    ok = good_text(text)
    return i, {
        "final_url": final_url,
        "http_status": status,
        "article_text": text if ok else None,
        "extractor": extractor if ok else (extractor or None),
        "error": None if ok else "Cookie wall or too short after AMP/Render/fallback",
        "word_count": len(text.split()) if text else 0
    }

# ======= MAIN =======
async def main():
    # Compat Windows
    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore
        except Exception:
            pass

    global renderer
    renderer = RenderExtractor()
    await renderer.start()

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

    df.to_csv(OUTPUT_CSV, index=False)
    try:
        df.to_parquet(OUTPUT_PARQUET, index=False)
    except Exception:
        pass

    await renderer.stop()

if __name__ == "__main__":
    asyncio.run(main())
