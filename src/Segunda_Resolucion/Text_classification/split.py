import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import joblib
from pathlib import Path

# =========================
# Configuración
# =========================
INPUT_CSV = "data/definitivos/INDEX_ALL_scrapped_filtrado.csv"
OUTPUT_DIR = "data/definitivos/splits"
TEST_SIZE = 0.2
RANDOM_STATE = 42

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# =========================
# 1. Cargar dataset
# =========================
df = pd.read_csv(INPUT_CSV)

# =========================
# 2. Crear texto de entrada
# =========================
df["text_input"] = df["headline"].fillna("") + " " + df["summary"].fillna("")

# =========================
# 3. Codificar etiquetas
# =========================
le = LabelEncoder()
df["label"] = le.fit_transform(df["topic"])

# Guardar label encoder
joblib.dump(le, f"{OUTPUT_DIR}/label_encoder.joblib")

# =========================
# 4. Split TRAIN / TEST
# =========================
train_df, test_df = train_test_split(
    df,
    test_size=TEST_SIZE,
    stratify=df["label"],
    random_state=RANDOM_STATE
)

# =========================
# 5. Guardar CSVs finales
# =========================
train_df.to_csv(f"{OUTPUT_DIR}/train.csv", index=False)
test_df.to_csv(f"{OUTPUT_DIR}/test.csv", index=False)

# =========================
# 6. Info rápida
# =========================
print("Split completado ✅")
print(f"Train: {len(train_df)} ejemplos")
print(f"Test : {len(test_df)} ejemplos")
print("\nDistribución TRAIN:")
print(train_df["topic"].value_counts())
print("\nDistribución TEST:")
print(test_df["topic"].value_counts())
