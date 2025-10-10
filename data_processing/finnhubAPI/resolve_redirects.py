#!/usr/bin/env python3
"""
resolve_redirects.py
--------------------
Lee un CSV con una columna 'url' (URLs de finnhub como https://finnhub.io/api/news?id=...)
y añade una nueva columna 'url_original' con la URL final del medio (publisher) tras seguir la redirección.

Uso:
    python resolve_redirects.py --input ticker_news.csv --output ticker_news_with_original.csv --workers 12 --sleep-min 0.2 --sleep-max 0.8

Requisitos:
    pip install requests pandas tqdm

Notas:
    - Respeta límites: pausas aleatorias y concurrencia moderada.
    - Guarda también el status y el HTTP code para depuración.
"""
import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm


def build_session(timeout: int = 15) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['HEAD', 'GET', 'OPTIONS'])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        "User-Agent": "PLN-EventCrawler/1.0 (+your_email@example.com)"
    })
    session.request_timeout = timeout
    return session


def resolve_one(url: str, session: requests.Session, sleep_min: float, sleep_max: float) -> dict:
    """Sigue redirecciones para obtener la URL final (publisher)."""
    try:
        # Pausa aleatoria breve para no saturar
        time.sleep(random.uniform(sleep_min, sleep_max))

        # Intento 1: HEAD (rápido, sigue redirects)
        try:
            r = session.head(url, allow_redirects=True, timeout=session.request_timeout)
            final_url = r.url
            code = r.status_code
            if final_url and final_url != url and code < 400:
                return {"url": url, "url_original": final_url, "status": "ok", "http_code": code}
        except Exception:
            pass

        # Intento 2: GET ligero (algunos servidores no manejan bien HEAD)
        r = session.get(url, allow_redirects=True, timeout=session.request_timeout)
        final_url = r.url
        code = r.status_code
        # No guardamos el contenido; solo nos interesa la URL final
        if final_url and code < 400:
            return {"url": url, "url_original": final_url, "status": "ok", "http_code": code}
        else:
            return {"url": url, "url_original": None, "status": "http_error", "http_code": code}
    except requests.exceptions.TooManyRedirects:
        return {"url": url, "url_original": None, "status": "too_many_redirects", "http_code": None}
    except requests.exceptions.Timeout:
        return {"url": url, "url_original": None, "status": "timeout", "http_code": None}
    except requests.exceptions.RequestException as e:
        return {"url": url, "url_original": None, "status": f"request_error:{type(e).__name__}", "http_code": None}
    except Exception as e:
        return {"url": url, "url_original": None, "status": f"error:{type(e).__name__}", "http_code": None}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Ruta al CSV de entrada con columna 'url'")
    parser.add_argument("--output", required=True, help="Ruta al CSV de salida")
    parser.add_argument("--workers", type=int, default=12, help="Número de hilos en paralelo")
    parser.add_argument("--sleep-min", type=float, default=0.2, help="Pausa mínima entre peticiones (s)")
    parser.add_argument("--sleep-max", type=float, default=0.8, help="Pausa máxima entre peticiones (s)")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    if "url" not in df.columns:
        raise ValueError("El CSV debe contener una columna llamada 'url'")

    urls = df["url"].dropna().astype(str).tolist()

    session = build_session()

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(resolve_one, u, session, args.sleep_min, args.sleep_max) for u in urls]
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Resolviendo redirecciones"):
            results.append(fut.result())

    res_df = pd.DataFrame(results)

    # Unimos por 'url' para añadir 'url_original', 'status', 'http_code'
    merged = df.merge(res_df, on="url", how="left")

    # Guardamos
    merged.to_csv(args.output, index=False, encoding="utf-8", lineterminator="\n")
    print(f"Listo. Salida en: {args.output}")


if __name__ == "__main__":
    main()
