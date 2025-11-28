# Documentacion de la clasificacion de noticias

## Shallow Learning

### Representaciones del lenguaje
- **TF-IDF**: se cargan 5 000 term features de la matriz TF-IDF generada previamente. Cada noticia queda en un vector disperso donde cada peso refleja la importancia relativa del termino en el documento. Se fija un split 80/10/10 (train/val/test) estratificado para comparar modelos en igualdad de condiciones y evitar sobreajuste al conjunto de validacion. TF-IDF se eligio como baseline porque privilegia terminos especificos del dominio financiero (pesando mas los que diferencian topics) y se comporta bien en textos cortos/medianos.
- **Word2Vec no contextual (skip-gram)**: se carga el modelo propio entrenado sobre el corpus financiero (16 863 terminos, 300 dimensiones). Cada texto se tokeniza, se toman los vectores de las palabras presentes y se promedia para obtener un unico vector denso de 300 dimensiones por documento. Se mantiene el mismo split 80/10/10 para comparar con TF-IDF. Se adopta el promedio porque es barato y evita depender de la longitud variable, pero se asume que perdera informacion de orden y de terminos raros.

### Modelos y resultados
- **Regresion Logistica multiclase**:
  - Configuracion: solver lbfgs (estable para multiclase), regularizacion L2 implicita, max_iter=1000 para asegurar convergencia en el espacio de 5 000 dimensiones. Es el baseline clasico para texto linealmente separable con pesos interpretables.
  - Con TF-IDF: val acc 0.9050, val macro-F1 0.8397; test acc 0.9050, test macro-F1 0.8245. El macro-F1 cercano al accuracy indica que las clases minoritarias no se quedan muy atras; se obtiene un modelo estable y rapido de entrenar.
  - Subsampling (TF-IDF): se balanceo el train recortando clases mayoritarias para probar mejora en minoritarias. Las metricas bajaron porque se pierde variabilidad util y el modelo generaliza peor; se descarta la estrategia.
  - Con Word2Vec promedio: val acc 0.8198, val macro-F1 0.7348; test acc 0.8236, test macro-F1 0.7085. El rendimiento baja porque el promedio de embeddings diluye terminos discriminativos y la LogReg no puede explotar relaciones semanticas sin pesos de frecuencia.
- **SVM lineal (LinearSVC)**:
  - Configuracion: hinge loss con C=1.0. Se elige SVM porque maximiza el margen en espacios de alta dimension, robusto a sobreajuste con datos dispersos y con buena capacidad para clases desbalanceadas cuando se optimiza el margen.
  - Con TF-IDF: val acc 0.9225, val macro-F1 0.8974; test acc 0.9283, test macro-F1 0.8716. El espacio de 5 000 dimensiones vuelve a las clases casi separables linealmente; la SVM extrae un hiperplano con margen amplio y domina en shallow.
  - Con Word2Vec promedio: val acc 0.8450, val macro-F1 0.8117; test acc 0.8566, test macro-F1 0.8027. Mejora frente a LogReg con los mismos vectores gracias al margen maximo, pero sigue por debajo de TF-IDF porque la señal semantica general no sustituye los pesos discriminativos por termino.

### Conclusiones de shallow
TF-IDF + SVM es la combinacion ganadora: usa terminos especificos ponderados y un clasificador que aprovecha la separabilidad lineal en alta dimension. Word2Vec promedio es mas compacto y semantico pero pierde detalle lexical util; solo mejora ligeramente con SVM respecto a LogReg. El subsampling no ayuda por la perdida de ejemplos de clases dominantes, asi que se conserva el train completo.

## Deep Learning

### Representacion secuencial con Word2Vec
- **Embeddings propios (skip-gram, 300d)**: se reutiliza el modelo financiero pero ahora se conserva el orden de palabras. Cada texto se tokeniza y se rellena (padding) o trunca hasta MAX_LEN=250 para equilibrar cobertura de palabras y coste de computo. Se construye un tensor (num_textos, 250, 300) con vectores por posicion. Las etiquetas se codifican con LabelEncoder y se usa un split estratificado 80/20 para train/test. Esta configuracion mantiene el contexto local y distribuye la misma semantica vista en shallow pero sin promediarla.
- **Embeddings preentrenados de Google News (300d)**: vectores para ~3 millones de palabras entrenados sobre ~100B palabras. Se genera la misma matriz secuencial (250 pasos, 300 dims) para comparar cobertura generalista frente a especializacion de dominio. Permite medir si la mayor cobertura y calidad general compensan la falta de vocabulario financiero especifico.

### Arquitecturas
- **CNN 1D**: Input -> Conv1D(128, kernel 5) -> GlobalMaxPooling -> Dense(64) -> Dropout 0.5 -> Softmax.
  - Razonamiento: kernel_size=5 captura patrones locales (n-gramas de 5) que suelen ser suficientes para detectar topics; GlobalMaxPooling hace la representacion invariante a la posicion, quedandose con el activacion mas fuerte por filtro; la capa densa combina patrones globales y el Dropout 0.5 mitiga overfitting dado el tamano moderado del dataset. Se usa Adam y sparse categorical crossentropy por estabilidad y porque las etiquetas estan codificadas como enteros. Se valida con 5-Fold CV (80/20 por fold), 5 epochs, batch 32 para balancear tiempo y robustez. Los accuracies medios con embeddings propios y con Google News son similares; la mayor cobertura de Google no compensa la especializacion financiera.
- **Bi-LSTM con pooling doble**: Input -> Bi-LSTM(128, return_sequences) -> GlobalMaxPooling + GlobalAveragePooling en paralelo -> concatenacion -> Dropout 0.5 -> Dense softmax.
  - Razonamiento: la Bi-LSTM modela dependencias largas en ambas direcciones, y el doble pooling captura tanto picos de activacion (max) como contexto global (mean) antes de la capa densa. Se replica el mismo regimen de 5-Fold CV, 5 epochs, batch 32 para comparar en igualdad. Los resultados quedan algo por debajo de la CNN y con mayor coste computacional; las dependencias largas no aportan ventaja clara frente a patrones locales para este conjunto.

### Comparacion y lecciones
- Dentro de Deep Learning, la CNN ofrece el mejor compromiso entre rendimiento y coste; la Bi-LSTM no supera a la CNN y tarda mas.
- Embeddings propios vs Google News arrojan desempenos parecidos: la especializacion en finanzas compensa la menor cobertura, y la cobertura generalista no aporta ganancia en este dominio.
- Frente a los modelos shallow, la CNN se acerca pero no supera al baseline TF-IDF+SVM. Para mejorar, opciones futuras serian mas epochs, variacion de kernel sizes, atencion o fine-tuning de embeddings, pero el modelo clasico ya es muy competitivo.
