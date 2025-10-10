# -*- coding: utf-8 -*-
"""
cookie_guard.py
Utilidades para detectar y limpiar boilerplate de consentimiento/cookies en texto y HTML.
Incluye heurísticas para decidir si forzar versión AMP/móvil.
"""
from __future__ import annotations
import re
import html as _html
from typing import Tuple

# --- Patrones para texto (ES/EN + CMP vendors + Yahoo banner) ---
COOKIE_PATTERNS = [
    r"\busamos\s+cookies\b",
    r"\bcuando\s+utilizas?\s+nuestros?\s+sitios?.*cookies",
    r"\bgestionar\s+configuraci[oó]n\s+de\s+privacidad\b",
    r"\brevocar\s+tu\s+consentimiento\b",
    r"\bmarco\s+de\s+transparencia\s+y\s+consentimiento\s+de\s+iab\b",
    r"\baceptar\s+todo\b|\brechazar\s+todo\b|\bconfigurar\s+cookies\b",
    r"\bpol[ií]tica\s+de\s+cookies?\b|\bpol[ií]tica\s+de\s+privacidad\b",
    r"\bwe\s+use\s+cookies\b|\byour\s+privacy\s+choices\b|\bmanage\s+preferences\b|\bprivacy\s+preferences\b",
    r"\bconsent\s+(preferences|choices|manager)\b",
    r"\bone\s*trust\b|\bquantcast\b|\btrustarc\b|\bdidomi\b|\bsourcepoint\b",
    r"\baccept\s+all\s+cookies\b|\breject\s+all\s+cookies\b",
    # Yahoo-specific consent banner
    r"\byahoo\s+is\s+part\s+of\s+the\s+yahoo\s+family\s+of\s+brands\b",
    r"\biab\s+transparency\s*&\s*consent\s*framework\b",
    # fragmento largo típico (capturado por los anteriores, pero lo dejamos por seguridad)
    r"cuando\s+utilizas?\s+nuestros?\s+sitios\s+y\s+aplicaciones,\s+usamos\s+cookies.*?pol[ií]tica\s+de\s+cookies",
]

# --- Marcadores HTML de cookie-wall ---
COOKIE_HTML_MARKERS = [
    "onetrust", "ot-sdk-container", "didomi", "qc-cmp2-container", "quantcast",
    "trustarc", "sourcepoint", "iab", "tcf", "consent", "cookie-banner",
    "manage privacy", "privacy choices", "cmp__container", "sp_message_container",
    # Yahoo consent interstitials
    "guce.yahoo.com", "consent.yahoo.com", "yahoo is part of the yahoo family of brands"
]

_COOKIE_RE = re.compile("|".join(COOKIE_PATTERNS), re.IGNORECASE | re.DOTALL)

def clean_cookie_text(text: str) -> Tuple[str, float, bool]:
    """
    Elimina párrafos de consentimiento/cookies y devuelve (texto_limpio, ratio_eliminado, flag_consent).
    flag_consent True si detectamos mucho boilerplate o si la limpieza reduce demasiado el contenido.
    """
    if not isinstance(text, str) or not text.strip():
        return text, 0.0, False
    t = _html.unescape(text)
    # cortar en párrafos
    paras = re.split(r"\n{2,}|\r{2,}|(?:(?<=[.!?])\s{2,})", t)
    if len(paras) <= 1:
        paras = re.split(r"\n|\r", t)
    total = sum(len(p) for p in paras) or 1
    keep, removed = [], 0
    for p in paras:
        if _COOKIE_RE.search(p):
            removed += len(p)
        else:
            keep.append(p)
    cleaned = "\n\n".join([p.strip() for p in keep if p.strip()])
    ratio = removed / total
    # flag si mucho boilerplate o si la palabra "cookie" aparece y el texto útil cayó fuerte
    flag = (ratio > 0.15) or ("cookie" in t.lower() and len(cleaned) < 0.75 * len(t))
    return cleaned if cleaned else t, ratio, flag

def looks_like_cookie_wall_text(text: str) -> bool:
    if not text:
        return False
    _, ratio, flag = clean_cookie_text(text)
    return flag or ratio > 0.2

def looks_like_cookie_wall_html(html: str) -> bool:
    if not html:
        return False
    h = html.lower()
    return any(marker in h for marker in COOKIE_HTML_MARKERS)
