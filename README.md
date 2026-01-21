# 📊 FinTracker  
Financial News Analysis with NLP

FinTracker es un proyecto de Procesamiento del Lenguaje Natural (NLP) orientado al análisis automático de noticias financieras. El objetivo principal es construir un pipeline completo que permita extraer, procesar, representar y analizar noticias económicas, combinando técnicas clásicas de NLP con modelos de deep learning basados en Transformers.

El proyecto cubre todo el flujo de trabajo típico en NLP aplicado a datos reales: desde la recolección de datos sin etiquetar, pasando por un preprocesado avanzado y la generación de embeddings, hasta tareas como la clasificación temática y la extracción de información relevante en el dominio financiero.

---

## 🧠 Objetivos del proyecto

- Construir un dataset propio de noticias financieras a partir de fuentes reales.
- Analizar y comparar distintos métodos de representación del lenguaje:
  - Representaciones no contextuales (Bag of Words, TF-IDF, Word2Vec).
  - Representaciones contextuales (BERT y FinBERT).
- Evaluar el impacto del preprocesado y la tokenización en cada tipo de embedding.
- Aplicar las representaciones obtenidas a tareas downstream como:
  - Clasificación temática de noticias financieras.
  - Análisis semántico en el dominio económico.
  - Extracción de entidades y métricas financieras relevantes.

## 🔄 Pipeline del sistema

### 1. Extracción de datos
- Descarga de noticias financieras mediante la API de Finnhub.
- Resolución de URLs originales para acceder al contenido real de los artículos.
- Scraping del texto principal de las noticias.
- Eliminación de textos inválidos, boilerplate y contenido irrelevante.

### 2. Preprocesado
- Limpieza y normalización del texto.
- Lematización utilizando spaCy.
- Sustitución de entidades numéricas por placeholders semánticos:
  - `__MONEY__`, `__PERCENT__`, `__YEAR__`, `__TICKER__`, etc.
- Diferenciación del preprocesado según el tipo de embedding:
  - Más agresivo para modelos no contextuales.
  - Conservador para modelos contextuales.

### 3. Representación del lenguaje
- Métodos tradicionales: Bag of Words y TF-IDF.
- Embeddings no contextuales: Word2Vec.
- Embeddings contextuales: BERT y FinBERT, adaptado al dominio financiero.

### 4. Análisis y aplicaciones
- Comparación cualitativa y cuantitativa de los embeddings generados.
- Clasificación temática de noticias financieras.
- Análisis del comportamiento semántico de los modelos en textos económicos reales.

---

## 🧪 Tecnologías utilizadas

- Python
- spaCy
- scikit-learn
- Hugging Face Transformers
- PyTorch
- Trafilatura
- TopicWizard
- Pandas, NumPy y Matplotlib

---

## 📈 Resultados destacados

- El preprocesado específico por tipo de embedding mejora de forma notable la calidad de las representaciones.
- Los modelos contextuales (BERT y FinBERT) capturan mejor relaciones semánticas complejas que los métodos tradicionales.
- FinBERT presenta un rendimiento superior en textos con lenguaje financiero frente a BERT generalista.
- Los métodos clásicos siguen siendo útiles como baseline y por su alta interpretabilidad.

---

## 👥 Autores

- Mateo Pérez Suárez  
- Iñigo Peña de las Heras  

Grado en Ciencia de Datos e Inteligencia Artificial  
Universidad de Deusto — Curso 2025-26

---

## 📌 Notas

Este proyecto tiene un enfoque académico y experimental, pero está diseñado con una estructura modular y reproducible, facilitando su extensión a nuevos modelos, datasets o tareas de NLP en el dominio financiero.


