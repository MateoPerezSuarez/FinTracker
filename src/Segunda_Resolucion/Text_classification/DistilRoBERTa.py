import os
import pandas as pd
import numpy as np
import torch
import joblib

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score

from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    Trainer,
    TrainingArguments,
)

# =========================
# Configuración
# =========================
TRAIN_CSV = "data/definitivos/splits/train.csv"
LABEL_ENCODER_PATH = "data/definitivos/splits/label_encoder.joblib"

MODEL_NAME = "distilroberta-base"
OUTPUT_DIR = "src/Segunda_Resolucion/Text_classification/models/distilroberta_topicclf"   # carpeta donde se guardará todo
VAL_SIZE = 0.1
RANDOM_STATE = 42
NUM_LABELS = 5

# =========================
# Utils
# =========================
def compute_metrics(eval_pred):
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=-1)
    return {
        "accuracy": accuracy_score(labels, preds),
        "f1_macro": f1_score(labels, preds, average="macro"),
        "f1_weighted": f1_score(labels, preds, average="weighted"),
    }

class FinanceDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {k: torch.tensor(v[idx]) for k, v in self.encodings.items()}
        item["labels"] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)

# =========================
# 1) Cargar datos
# =========================
df = pd.read_csv(TRAIN_CSV)

# Asegurar que existen columnas esperadas
if "text_input" not in df.columns:
    df["text_input"] = df["headline"].fillna("") + " " + df["summary"].fillna("")

if "label" not in df.columns:
    raise ValueError("No existe columna 'label' en train.csv. Genera splits con el script previo.")

texts = df["text_input"].astype(str).tolist()
labels = df["label"].astype(int).tolist()

# =========================
# 2) Split TRAIN/VAL (desde TRAIN)
# =========================
train_texts, val_texts, train_labels, val_labels = train_test_split(
    texts,
    labels,
    test_size=VAL_SIZE,
    stratify=labels,
    random_state=RANDOM_STATE
)

# =========================
# 3) Tokenización
# =========================
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

def tokenize_function(batch_texts):
    return tokenizer(
        batch_texts,
        padding="max_length",
        truncation=True,
        max_length=512
    )

train_encodings = tokenize_function(train_texts)
val_encodings = tokenize_function(val_texts)

train_dataset = FinanceDataset(train_encodings, train_labels)
val_dataset = FinanceDataset(val_encodings, val_labels)

# =========================
# 4) Modelo
# =========================
model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_NAME,
    num_labels=NUM_LABELS
)

# =========================
# 5) TrainingArguments
# OJO: en transformers nuevos es eval_strategy (no evaluation_strategy)
# =========================
training_args = TrainingArguments(
    output_dir=os.path.join(OUTPUT_DIR, "checkpoints"),
    num_train_epochs=3,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    gradient_accumulation_steps=2,
    learning_rate=4e-5,
    weight_decay=0.01,
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    metric_for_best_model="f1_macro",
    greater_is_better=True,
    logging_dir=os.path.join(OUTPUT_DIR, "logs"),
    logging_steps=50,
    report_to="none",  # evita integrar con wandb por defecto
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    tokenizer=tokenizer,
    compute_metrics=compute_metrics,
)

# =========================
# 6) Entrenar
# =========================
trainer.train()

# =========================
# 7) Guardar modelo + tokenizer + label encoder
# =========================
os.makedirs(OUTPUT_DIR, exist_ok=True)

trainer.save_model(OUTPUT_DIR)          # modelo + config
tokenizer.save_pretrained(OUTPUT_DIR)   # tokenizer

# Guarda también el label encoder (copiándolo al dir del modelo)
le = joblib.load(LABEL_ENCODER_PATH)
joblib.dump(le, os.path.join(OUTPUT_DIR, "label_encoder.joblib"))

print(f"\nModelo guardado en: {OUTPUT_DIR}")
