import pandas as pd

INPUT_FILE = 'data_processing/finnhubAPI/data/porEmpresas/definitivos/AAPL_scrapped' 
OUTPUT_FILE = 'data_processing/finnhubAPI/data/porEmpresas/definitivos/APPL_scrapped_filtrado.csv'
URL_COLUMN = 'url_original' 
NEW_COLUMN = 'article_text' 
MIN_TEXT_LENGTH = 100
# Artefactos (texto no deseado) que indican una extracción fallida o parcial.
ERROR_ARTIFACTS = [
    "oops, something went wrong",
    "all rights reserved.",
    "terms and privacy policy",
    "my portfolio",
    "watch now",
    "min read",
    "scott lehtonen", # Incluimos nombres o firmas comunes en los errores de Yahoo
    "patrick seitz"
]
# ---------------------

def check_text_quality(text):
    """
    Evalúa la calidad del texto extraído: longitud mínima y presencia de artefactos de error.
    Devuelve True si el texto es válido, False si debe ser eliminado.
    """
    # Si el valor es nulo (NaN/None), lo eliminamos
    if pd.isna(text):
        return False 
    
    text = str(text)
    
    # 1. Validación de Longitud Mínima
    if len(text) < MIN_TEXT_LENGTH:
        return False
        
    # 2. Validación de Artefactos de Error
    text_lower = text.lower()
    for artifact in ERROR_ARTIFACTS:
        if artifact in text_lower:
            # Si encontramos un artefacto (como "oops, something went wrong"), eliminamos la fila
            return False 
            
    # El texto es considerado de buena calidad
    return True 


# --- EJECUCIÓN DEL SCRIPT ---
print(f"Cargando dataset con errores: {INPUT_FILE}")
try:
    df = pd.read_csv(INPUT_FILE)
except FileNotFoundError:
    print(f"ERROR: Archivo no encontrado. Asegúrate de que '{INPUT_FILE}' está en la misma carpeta.")
    exit()

initial_rows = len(df)
print(f"Filas iniciales: {initial_rows}")

# Aplica la función de chequeo de calidad y usa el resultado para filtrar el DataFrame
print("\nFiltrando filas por artefactos de error y longitud mínima...")
df['is_valid'] = df[NEW_COLUMN].apply(check_text_quality)
df_cleaned = df[df['is_valid']].drop(columns=['is_valid'])

final_rows = len(df_cleaned)
rows_dropped = initial_rows - final_rows

print(f"Filas finales después de la limpieza: {final_rows}")
print(f"Filas eliminadas (por errores o texto incompleto): {rows_dropped}")

# Guardar el nuevo dataset limpio
df_cleaned.to_csv(OUTPUT_FILE, index=False)

print(f"Dataset limpio generado con éxito como: {OUTPUT_FILE}")
print(f"Ejemplo de las primeras 3 entradas limpias:\n")
print(df_cleaned[[URL_COLUMN, NEW_COLUMN]].head(3))