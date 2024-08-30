import os
import json
import re
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
from nltk.corpus import stopwords
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurar el path del ejecutable de Tesseract si no está en el PATH del sistema
# pytesseract.pytesseract.tesseract_cmd = r'/opt/homebrew/bin/tesseract'

# Cargar stopwords en español
stop_words = set(stopwords.words('spanish'))

def pdf_to_text(pdf_path):
    """ Convertir PDF a texto usando OCR. """
    images = convert_from_path(pdf_path)
    text = ''
    for image in images:
        text += pytesseract.image_to_string(image)
    return text

def process_pdf_file(pdf_path):
    """ Procesar un archivo PDF y devolver el texto extraído. """
    text = pdf_to_text(pdf_path)
    
    # Normalizar texto
    text = text.lower()
    words = re.findall(r'\b\w+\b', text)
    
    # Eliminar stopwords
    filtered_words = [word for word in words if word not in stop_words]
    
    return filtered_words

def build_inverted_index_parallel(folder_path):
    """ Construir el índice invertido a partir de PDFs en una carpeta utilizando paralelismo. """
    inverted_index = {}
    pdf_files = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith('.pdf')]
    total_files = len(pdf_files)
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_pdf_file, pdf_path): pdf_path for pdf_path in pdf_files}
        for i, future in enumerate(as_completed(futures)):
            pdf_path = futures[future]
            filename = os.path.basename(pdf_path)
            try:
                filtered_words = future.result()
                for word in filtered_words:
                    if word not in inverted_index:
                        inverted_index[word] = []
                    if filename not in inverted_index[word]:
                        inverted_index[word].append(filename)
            except Exception as e:
                print(f"Error procesando {pdf_path}: {e}")
            
            # Mostrar mensaje de progreso cada 10%
            if (i + 1) % (total_files // 10) == 0:
                print(f"Progreso: {(i + 1) / total_files * 100:.0f}% completado")
    
    return inverted_index

def normalizar_palabra(palabra):
    """ Normalizar la palabra eliminando ceros a la izquierda, guiones bajos y caracteres especiales. """
    # Eliminar caracteres especiales excepto letras y números
    palabra = re.sub(r'[^\w\s]', '', palabra)
    # Eliminar ceros a la izquierda
    palabra = palabra.lstrip('0')
    # Eliminar guiones bajos al inicio y al final
    palabra = palabra.strip('_')
    #eliminar :
    palabra = palabra.strip(':')
    #eliminar ;
    palabra = palabra.strip(';')
    return palabra

def procesar_indice(inverted_index):
    """ Procesar el índice invertido para normalizar palabras. """
    nuevo_indice = {}
    palabras_vistas = set()

    for palabra, documentos in inverted_index.items():
        palabra_normalizada = normalizar_palabra(palabra)
        
        if palabra_normalizada not in palabras_vistas:
            nuevo_indice[palabra_normalizada] = documentos
            palabras_vistas.add(palabra_normalizada)

    return nuevo_indice

def save_inverted_index_to_json(inverted_index, output_path):
    """ Guardar el índice invertido en un archivo JSON. """
    sorted_index = dict(sorted(inverted_index.items()))
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json.dump(sorted_index, json_file, ensure_ascii=False, indent=4)

def save_inverted_index_to_mongodb(inverted_index, db_name, collection_name):
    """ Guardar el índice invertido en MongoDB. """
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    documents = [{'word': word, 'files': files} for word, files in inverted_index.items()]
    collection.insert_many(documents)

def load_json_to_mongodb(json_path, db_name, collection_name):
    """ Cargar un archivo JSON en MongoDB. """
    with open(json_path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)
    inverted_index = [{'word': word, 'files': files} for word, files in data.items()]
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(inverted_index)

# Configurar paths
folder_path = r"C:\Users\56974\Desktop\seminario 2024\codigo python github\decretos_2023_test"
output_path = r"C:\Users\56974\Desktop\seminario 2024\codigo python github\indice_invertido_con_stopwords_normalizados_github.json"

# Nombre de la base de datos y colección en MongoDB
db_name = 'indice_invertido_decretos_munvalp_test_github'
collection_name = 'indice_invertido_test_github'

# Verificar si el archivo JSON ya existe
if os.path.exists(output_path):
    load_json_to_mongodb(output_path, db_name, collection_name)
else:
    inverted_index = build_inverted_index_parallel(folder_path)
    procesado_indice = procesar_indice(inverted_index)
    save_inverted_index_to_json(procesado_indice, output_path)
    save_inverted_index_to_mongodb(procesado_indice, db_name, collection_name)
