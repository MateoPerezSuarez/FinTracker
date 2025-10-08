import pandas as pd
import pandas as pd, re, unicodedata
from difflib import SequenceMatcher

INPUT = "data_processing/ticker_news_with_text.csv"
OUTPUT = "data_processing/pruebas/ticker_news_with_text_filtered.csv"

MIN_WORDS_KEEP = 80          # mínimo de palabras para conservar (ajusta)
FUZZY_THRESHOLD = 0.60       # similitud mínima con plantilla para marcar cookie wall

# --- Plantilla del muro (la tuya, en ES) ---
COOKIE_WALL_ES = """Cuando utilizas nuestros sitios y aplicaciones, usamos cookies para:
- proporcionarte nuestros sitios y aplicaciones;
- autenticar usuarios, aplicar medidas de seguridad y evitar el spam y los abusos, y
- medir su uso de nuestros sitios y aplicaciones
Si haces clic en «
Aceptar todo», nosotros y
nuestros socios, incluidos los 237 que son parte del Marco de transparencia y consentimiento de IAB, también almacenaremos o guardaremos información en un dispositivo (en otras palabras, usaremos cookies) y utilizaremos datos de geolocalización y otros tipo de datos precisos, como direcciones IP, y datos de navegación y de búsquedas, con el fin de efectuar análisis, mostrar anuncios y contenidos personalizados, medir la publicidad y el contenido, realizar investigaciones de públicos y desarrollar servicios.
Si no quieres que nosotros ni nuestros socios utilicemos cookies y datos personales para estos propósitos adicionales, haz clic en «Rechazar todo».
Si quieres personalizar tus opciones, haz clic en «Gestionar configuración de privacidad».
Puedes revocar tu consentimiento o cambiar tus opciones en cualquier momento haciendo clic en el enlace «Configuración de privacidad y cookies» o «Panel de privacidad» de nuestros sitios y aplicaciones. Para obtener más información sobre cómo utilizamos tus datos personales, consulta nuestra Política de privacidad y la Política de cookies."""

# --- Normalización agresiva ---
def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    # quitar acentos
    t = unicodedata.normalize("NFD", text)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    # minúsculas y quitar símbolos
    t = t.lower()
    t = re.sub(r"[^\w\s]", " ", t)   # deja letras/dígitos/espacios
    t = re.sub(r"\s+", " ", t).strip()
    return t

COOKIE_NORM = normalize(COOKIE_WALL_ES)

# --- Palabras/expresiones clave (ES + EN) ---
KEYWORDS = [
    # ES
    "usamos cookies","utilizamos cookies","politica de cookies","politica de privacidad",
    "aceptar todo","rechazar todo","gestionar configuracion de privacidad","panel de privacidad",
    "configuracion de privacidad y cookies","marco de transparencia y consentimiento",
    # EN
    "we use cookies","cookie policy","privacy policy","accept all","reject all",
    "manage privacy settings","privacy dashboard","privacy cookie settings",
    "iab transparency consent framework"
]

def keyword_hits(norm_text: str) -> int:
    return sum(1 for kw in KEYWORDS if kw in norm_text)

def is_cookie_wall(text: str) -> bool:
    if not isinstance(text, str) or not text.strip():
        return False
    norm = normalize(text)

    # 1) Fuzzy con la plantilla ES
    fuzz = SequenceMatcher(None, COOKIE_NORM, norm).ratio()
    if fuzz >= FUZZY_THRESHOLD:
        return True

    # 2) Heurística por palabras clave:
    hits = keyword_hits(norm)
    # Reglas: muchas paredes traen varias menciones a cookies + opciones
    has_cookies  = any(k in norm for k in ["cookie", "cookies"])
    has_actions  = any(k in norm for k in ["accept all","reject all","aceptar todo","rechazar todo"])
    has_privacy  = any(k in norm for k in ["privacy","privacidad"])
    if (hits >= 3 and has_cookies and (has_actions or has_privacy)):
        return True

    # 3) Si el texto empieza directamente con bloques de privacidad y es corto
    starts_priv = norm[:300].find("cookie") != -1 or norm[:300].find("privac") != -1
    if starts_priv and hits >= 2 and len(norm.split()) < 600:
        return True

    return False

# --- Carga y filtrado ---
df = pd.read_csv(INPUT)

# Normaliza condiciones de guardado
df["article_text"] = df["article_text"].fillna("")

# Clasifica
df["is_cookie_wall"] = df["article_text"].apply(is_cookie_wall)

# Filtrado final:
# - texto no vacío
# - no cookie wall
# - longitud mínima (usa word_count si lo tienes; si no, calcula)
if "word_count" in df.columns:
    word_len = df["word_count"].fillna(0)
else:
    word_len = df["article_text"].apply(lambda t: len(normalize(t).split()))

mask_keep = (
    (df["article_text"].str.strip() != "")
    & (~df["is_cookie_wall"])
    & (word_len >= MIN_WORDS_KEEP)
)

filtered = df[mask_keep].copy()

# Guarda resultado
filtered.to_csv(OUTPUT, index=False)

# Reporte rápido
print(f"Total filas: {len(df)}")
print(f"Marcadas cookie wall: {df['is_cookie_wall'].sum()}")
print(f"Conservadas: {len(filtered)}")


