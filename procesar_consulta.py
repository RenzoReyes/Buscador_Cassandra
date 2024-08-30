import sys
import json
import numpy as np
from pymongo import MongoClient
from transformers import BertTokenizer, BertModel
import torch

# Ruta de los archivos de documentos
RUTA_DOCUMENTOS = r'C:\Users\56974\Desktop\seminario 2024\codigo python github\decretos_2023_test'

# Ruta del archivo de embeddings precalculados
RUTA_EMBEDDINGS = r'C:\Users\56974\Desktop\seminario 2024\codigo python github\embeddings.npy'

# Verificar si se pasó una consulta como argumento
if len(sys.argv) < 2:
    print("Error: No se proporcionó ninguna consulta.")
    sys.exit(1)

query = sys.argv[1]

# Configuración de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['indice_invertido_decretos_munvalp_test_github']
collection = db['indice_invertido_test_github']

# Función para obtener el índice invertido de MongoDB
def buscar_en_indice_invertido(query):
    resultado = collection.find_one({"word": query})
    if resultado:
        return resultado['files']
    return []

# Función para obtener embeddings de BERT
def obtener_embeddings(texto, modelo, tokenizador):
    inputs = tokenizador(texto, return_tensors='pt', truncation=True, padding=True)
    outputs = modelo(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()

# Inicializar BERT
tokenizador = BertTokenizer.from_pretrained('bert-base-uncased')
modelo = BertModel.from_pretrained('bert-base-uncased')

try:
    # Cargar los embeddings precalculados
    embeddings_dict = np.load(RUTA_EMBEDDINGS, allow_pickle=True).item()

    doc_ids = buscar_en_indice_invertido(query)
    if not doc_ids:
        print(json.dumps([]))
        sys.exit(0)

    # Obtener embedding de la consulta
    embedding_consulta = obtener_embeddings(query, modelo, tokenizador)

    # Calcular similitudes
    resultados = []
    for doc_id in doc_ids:
        embedding_doc = embeddings_dict.get(doc_id)
        if embedding_doc is not None:
            similitud = (embedding_consulta @ embedding_doc.T).flatten()[0]
            resultados.append({'_id': doc_id, 'similitud': float(similitud)})

    # Ordenar por similitud
    resultados_ordenados = sorted(resultados, key=lambda x: x['similitud'], reverse=True)
    print(json.dumps(resultados_ordenados))

except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)