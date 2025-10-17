import pandas as pd
from nltk.stem import WordNetLemmatizer
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import CountVectorizer
from topicwizard.pipeline import make_topic_pipeline
import topicwizard
import re

df = pd.read_csv("data_processing/finnhubAPI/data/porEmpresas/definitivos/INDEX_ALL_scrapped_filtrado.csv")
print(len(df))

lemmatizer = WordNetLemmatizer()

def tokenize_and_lemmatize(text):
    if pd.isna(text):
        return []
    text = re.sub(r'[^a-zA-Z\s]', '', str(text))
    tokens = text.lower().split() # Case Folding + Tokenization
    lemmas = [lemmatizer.lemmatize(t) for t in tokens]
    return lemmas

texts = df["article_text"].dropna().tolist()

# CountVectorizer
cv = CountVectorizer(tokenizer = tokenize_and_lemmatize, stop_words = 'english')

# NMF
nmf = NMF(n_components=5, random_state=42)

# Create a pipeline
topic_pipeline = make_topic_pipeline(cv, nmf, pandas_out=True)
topic_pipeline.fit(texts)

topic_vectors = topic_pipeline.transform(texts)
df["topic"] = topic_vectors.idxmax(axis=1)

df.to_csv("data_processing/finnhubAPI/data/porEmpresas/definitivos/INDEX_ALL_scrapped_filtrado.csv", index=False)