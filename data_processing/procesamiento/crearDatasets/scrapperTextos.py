import pandas as pd
import trafilatura
from tqdm import tqdm
import time
import pathlib
import os

tqdm.pandas()

# --- CONFIGURACIÓN DE RUTAS Y PARÁMETROS ---

# Directorio donde se encuentran los archivos CSV originales (ej. AMZN_orig.csv, AAPL_orig.csv)
# Se asume que la ruta es relativa a donde ejecutas el script.
ROOT_DIR = 'PRUEBABuena/FINNHUB/data' 

# Subdirectorio donde se guardarán los resultados limpios
OUTPUT_SUBDIR = 'scrappedData' 
OUTPUT_DIR = pathlib.Path(ROOT_DIR) / OUTPUT_SUBDIR 

URL_COLUMN = 'url_original'
NEW_COLUMN = 'article_text'

#Desechar textos de menos de 100 caracteres, hemos tenido errores con ello.
MIN_TEXT_LENGTH = 100 

#lista de elementos que he encontrado en los errores, y para filtrarlos
ERROR_ARTIFACTS = [
    "oops, something went wrong",
    "all rights reserved.",
    "terms and privacy policy",
    "my portfolio",
    "watch now",
    "min read",
    "scott lehtonen", 
    "patrick seitz",
    "- my portfolio",
    "copyright"
]

# --- FUNCIÓN DE EXTRACCIÓN CON FILTRADO ---

def extract_main_text(url):
    """
    Extrae el texto del artículo, valida su calidad (longitud y artefactos de error),
    y devuelve el texto limpio o None si la fila debe ser eliminada.
    """
    time.sleep(0.5) 
    
    try:
        # 1. Descargar el contenido de la URL
        downloaded = trafilatura.fetch_url(url)
        
        if not downloaded:
            return None
            
        # 2. Analizar el contenido y extraer solo el texto principal
        extracted_text = trafilatura.extract(
            downloaded, 
            favor_recall=True, 
            include_comments=False, 
            output_format='txt'
        )
        
        # Si no hay texto o es muy corto, fallamos
        if not extracted_text or len(extracted_text) < MIN_TEXT_LENGTH:
            return None
        
        # 3. Validación de Artefactos de Error
        text_lower = extracted_text.lower()
        for artifact in ERROR_ARTIFACTS:
            if artifact in text_lower:
                return None # Eliminamos si se encuentra un artefacto
        
        # Si pasa todas las validaciones, el texto es válido
        return extracted_text
    
    except Exception as e:
        # Captura cualquier error de red o librería y lo marca para eliminación
        print(f"Error al procesar {url}: {e}", flush=True) # flush=True para imprimir inmediatamente
        return None

# --- EJECUCIÓN DEL PROCESAMIENTO ---

# Crear el directorio de salida si no existe
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print(f"Directorio de salida creado/verificado: {OUTPUT_DIR}")

# Buscar todos los archivos que coincidan con el patrón en el ROOT_DIR
input_files = list(pathlib.Path(ROOT_DIR).glob('*_orig.csv'))

if not input_files:
    print(f"\nERROR: No se encontraron archivos *_orig.csv en el directorio: {ROOT_DIR}")
    exit()

print(f"\nSe encontraron {len(input_files)} archivos para procesar.")

for input_path in input_files:
    # 1. Determinar rutas de archivo y nombres
    input_filename = input_path.name
    ticker = input_filename.split('_orig')[0] 
    output_filename = f'{ticker}_scrapped_filtrado.csv'
    output_path = OUTPUT_DIR / output_filename
    
    print("-" * 50)
    print(f"PROCESANDO: {input_filename}")
    
    try:
        # 2. Cargar el dataset
        df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Error al cargar {input_filename}: {e}. Saltando archivo.")
        continue

    initial_rows = len(df)
    print(f"Filas iniciales: {initial_rows}")

    # 3. Aplicar el web scraping y guardar el resultado
    print("Iniciando extracción de texto...")
    df[NEW_COLUMN] = df[URL_COLUMN].progress_apply(extract_main_text)

    # 4. Limpieza: Eliminar filas donde la extracción falló (valor es None)
    print("Aplicando filtros de calidad y artefactos de error...")
    df_cleaned = df.dropna(subset=[NEW_COLUMN])

    final_rows = len(df_cleaned)
    rows_dropped = initial_rows - final_rows

    # 5. Guardar el nuevo dataset limpio
    df_cleaned.to_csv(output_path, index=False)

    print(f"PROCESAMIENTO COMPLETO para {ticker}:")
    print(f"  Filas eliminadas: {rows_dropped}")
    print(f"  Filas finales: {final_rows}")
    print(f"  Guardado en: {output_path}")

print("\n--- PROCESAMIENTO MASIVO FINALIZADO ---")