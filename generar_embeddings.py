import os
import numpy as np
import pytesseract
from pdf2image import convert_from_path
from transformers import BertTokenizer, BertModel

# Configuración de Tesseract si es necesario
# pytesseract.pytesseract.tesseract_cmd = '/usr/local/bin/tesseract'  # Ajusta según el sistema

# Ruta de la carpeta donde se encuentran los documentos PDF
RUTA_DOCUMENTOS = r'C:\Users\56974\Desktop\seminario 2024\codigo python github\decretos_2023_test'
OUTPUT_PATH = r'C:\Users\56974\Desktop\seminario 2024\codigo python github\embeddings.npy'

# Inicializar BERT
tokenizador = BertTokenizer.from_pretrained('bert-base-uncased')
modelo = BertModel.from_pretrained('bert-base-uncased')

# Función para obtener embeddings de BERT
def obtener_embeddings(texto, modelo, tokenizador):
    inputs = tokenizador(texto, return_tensors='pt', truncation=True, padding=True)
    outputs = modelo(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()

# Función para extraer texto de un PDF
def pdf_to_text(pdf_path):
    images = convert_from_path(pdf_path)
    text = ''
    for image in images:
        text += pytesseract.image_to_string(image)
    return text

# Calcular y guardar los embeddings
embeddings_dict = {}
for filename in os.listdir(RUTA_DOCUMENTOS):
    if filename.endswith('.pdf'):
        doc_id = os.path.splitext(filename)[0]
        pdf_path = os.path.join(RUTA_DOCUMENTOS, filename)
        try:
            text = pdf_to_text(pdf_path)
            embedding = obtener_embeddings(text, modelo, tokenizador)
            embeddings_dict[doc_id] = embedding
        except Exception as e:
            print(f"Error al procesar {filename}: {e}")

# Guardar los embeddings en un archivo .npy
np.save(OUTPUT_PATH, embeddings_dict)
print(f"Embeddings guardados en {OUTPUT_PATH}")