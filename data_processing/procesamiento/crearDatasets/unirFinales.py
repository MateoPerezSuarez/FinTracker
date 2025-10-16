import pandas as pd
import glob
import os

# Carpeta donde están los archivos CSV (puedes ajustar la ruta)
folder_path = r"data_processing/finnhubAPI/data/porEmpresas/definitivos"  # 🔹 CAMBIA esto por la ruta donde están los archivos

# Buscar todos los CSV que terminen en '_scrapped_filtrado.csv'
csv_files = glob.glob(os.path.join(folder_path, "*_scrapped_filtrado.csv"))

# Lista para almacenar los DataFrames
dataframes = []

# Leer y concatenar todos los CSV
for file in csv_files:
    print(f"Leyendo: {os.path.basename(file)}")
    df = pd.read_csv(file)
    dataframes.append(df)

# Concatenar todo en un solo DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# Guardar en un nuevo archivo
output_path = os.path.join(folder_path, "INDEX_ALL_scrapped_filtrado.csv")
combined_df.to_csv(output_path, index=False)

print(f"Archivos combinados correctamente en: {output_path}")
