import sys
import json
import numpy as np
import math
import os
from collections import Counter
from transformers import BertTokenizer, BertModel

# Ruta de los archivos de documentos
RUTA_DOCUMENTOS = "/Users/renzo/Desktop/seminario/decretos_2023_test"

# Ruta del archivo de embeddings precalculados
RUTA_EMBEDDINGS = '/Users/renzo/Desktop/seminario/Buscador/embeddings.npy'

if len(sys.argv) < 2:
    print("Error: No se proporcionaron la consulta o la ruta de documentos.")
    sys.exit(1)

query = sys.argv[1].strip()  # Eliminar espacios en blanco alrededor de la consulta

# Función para obtener embeddings de BERT
def obtener_embeddings(texto, modelo, tokenizador):
    inputs = tokenizador(texto, return_tensors='pt', truncation=True, padding=True)
    outputs = modelo(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()

# Función para calcular TF-IDF
def calcular_tf_idf(termino, documento, documentos):
    tf = documento.count(termino) / len(documento)
    idf = math.log(len(documentos) / sum([1 for doc in documentos if termino in doc]))
    return tf * idf

# Función para calcular la similitud basada en TF-IDF
def similitud_tf_idf(query, documento, documentos):
    terms_query = Counter(query.split())
    terms_doc = Counter(documento.split())
    
    score = 0.0
    for term in terms_query:
        if term in terms_doc:
            score += calcular_tf_idf(term, documento, documentos) * calcular_tf_idf(term, query, documentos)
    return score

# Inicializar BERT
print("Cargando modelo BERT...")
tokenizador = BertTokenizer.from_pretrained('bert-base-uncased')
modelo = BertModel.from_pretrained('bert-base-uncased')
print("Modelo BERT cargado correctamente.")

try:
    # Verificar que el archivo de embeddings existe
    if not os.path.exists(RUTA_EMBEDDINGS):
        raise FileNotFoundError(f"No se encontró el archivo de embeddings en la ruta {RUTA_EMBEDDINGS}")

    # Cargar los embeddings precalculados
    print("Cargando embeddings precalculados...")
    embeddings_dict = np.load(RUTA_EMBEDDINGS, allow_pickle=True).item()
    print("Embeddings cargados correctamente.")

    # Verificar que la consulta no esté vacía
    if not query:
        raise ValueError("La consulta proporcionada está vacía.")

    # Obtener embedding de la consulta
    print("Calculando embeddings para la consulta...")
    embedding_consulta = obtener_embeddings(query, modelo, tokenizador)
    print("Embeddings para la consulta calculados.")

    # Leer todos los documentos
    documentos = {}
    for doc_id, embedding_doc in embeddings_dict.items():
        with open(os.path.join(RUTA_DOCUMENTOS, f"{doc_id}.txt"), 'r', encoding='utf-8') as file:
            documentos[doc_id] = file.read()

    # Calcular similitudes
    resultados = []
    for doc_id, embedding_doc in embeddings_dict.items():
        similitud_bert = (embedding_consulta @ embedding_doc.T).flatten()[0]
        similitud_tfidf = similitud_tf_idf(query, documentos[doc_id], documentos.values())
        score_combined = 0.5 * similitud_bert + 0.5 * similitud_tfidf  # Combina ambas similitudes
        resultados.append({'_id': doc_id, 'similitud': float(score_combined)})

    # Ordenar por similitud
    resultados_ordenados = sorted(resultados, key=lambda x: x['similitud'], reverse=True)
    print("Similitudes calculadas y documentos ordenados.")
    print(json.dumps(resultados_ordenados))

except FileNotFoundError as fnfe:
    print(f"Error: {fnfe}", file=sys.stderr)
    sys.exit(1)
except ValueError as ve:
    print(f"Error: {ve}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error inesperado: {e}", file=sys.stderr)
    sys.exit(1)
