import requests, zipfile, io, os

os.makedirs("data/external", exist_ok=True)
url = "https://nlp.stanford.edu/data/glove.6B.zip"
zip_path = "src/Representacion_del_lenguaje/instalacionesglove.6B.zip"

# Descargar (~822MB)
r = requests.get(url)
with open(zip_path, "wb") as f:
    f.write(r.content)

# Extraer solo el de 300d
with zipfile.ZipFile(zip_path, "r") as z:
    z.extract("glove.6B.300d.txt", "data/external/")

print("Descargado y extraído glove.6B.300d.txt ✅")
