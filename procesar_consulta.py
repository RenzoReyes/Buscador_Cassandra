import sys
import json
import numpy as np
from cassandra.cluster import Cluster
from transformers import BertTokenizer, BertModel

# Ruta del archivo de embeddings precalculados
RUTA_EMBEDDINGS = '/Users/renzo/Desktop/seminario/Buscador/embeddings.npy'

if len(sys.argv) < 2:
    print("Error: No se proporcionó ninguna consulta.")
    sys.exit(1)

query = sys.argv[1]

# Configuración de Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('indice_invertido_keyspace')

# Función para obtener el índice invertido de Cassandra
def buscar_en_indice_invertido(session, query):
    try:
        resultados = session.execute("SELECT files FROM indice_invertido_table WHERE word = %s", [query])
        files = []
        for row in resultados:
            files.extend(row.files)  # 'files' es un campo tipo LIST<TEXT> en Cassandra
        return files
    except Exception as e:
        print(f"Error al buscar en el índice invertido: {e}", file=sys.stderr)
        return []

# Función para obtener embeddings de BERT
def obtener_embeddings(texto, modelo, tokenizador):
    inputs = tokenizador(texto, return_tensors='pt', truncation=True, padding=True)
    outputs = modelo(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()

# Inicializar BERT
print("Inicializando BERT...")
tokenizador = BertTokenizer.from_pretrained('bert-base-uncased')
modelo = BertModel.from_pretrained('bert-base-uncased')
print("Modelo BERT cargado correctamente.")

try:
    # Cargar los embeddings precalculados
    print("Cargando embeddings precalculados...")
    embeddings_dict = np.load(RUTA_EMBEDDINGS, allow_pickle=True).item()
    print("Embeddings cargados correctamente.")

    doc_ids = buscar_en_indice_invertido(session, query)
    print("Documentos encontrados para la consulta:", doc_ids)  # Depuración

    if not doc_ids:
        print("No se encontraron documentos para la consulta.")
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
        else:
            print(f"Advertencia: No se encontró embedding para el documento {doc_id}.")

    # Ordenar por similitud
    resultados_ordenados = sorted(resultados, key=lambda x: x['similitud'], reverse=True)
    print("Similitudes calculadas y documentos ordenados.")
    print(json.dumps(resultados_ordenados))

except Exception as e:
    print(f"Error inesperado: {e}", file=sys.stderr)
    sys.exit(1)

finally:
    # Cerrar la conexión a Cassandra
    session.shutdown()
    cluster.shutdown()
