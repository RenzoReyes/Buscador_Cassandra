import os
import time
import threading
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
import json
import re
from concurrent.futures import ThreadPoolExecutor

# Configurar el path del ejecutable de Tesseract si no está en el PATH del sistema
# pytesseract.pytesseract.tesseract_cmd = '/usr/local/bin/tesseract'  # Ajusta según el resultado de `which tesseract`

def pdf_to_text(pdf_path):
    images = convert_from_path(pdf_path)
    text = ''
    for image in images:
        text += pytesseract.image_to_string(image)
    return text

def load_inverted_index(output_path):
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as json_file:
            return json.load(json_file)
    return {}

def update_inverted_index(inverted_index, pdf_path):
    text = pdf_to_text(pdf_path)
    text = text.lower()
    words = re.findall(r'\b\w+\b', text)
    filename = os.path.basename(pdf_path)
    for word in words:
        if word not in inverted_index:
            inverted_index[word] = []
        if filename not in inverted_index[word]:
            inverted_index[word].append(filename)
    return inverted_index

def save_inverted_index_to_json(inverted_index, output_path):
    sorted_index = dict(sorted(inverted_index.items()))
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json.dump(sorted_index, json_file, ensure_ascii=False, indent=4)

def process_pdf(pdf_path, inverted_index, output_path):
    """Función para procesar un archivo PDF y actualizar el índice invertido."""
    print(f'Procesando archivo: {pdf_path}')
    update_inverted_index(inverted_index, pdf_path)
    save_inverted_index_to_json(inverted_index, output_path)

def check_for_new_files(folder_path, inverted_index, output_path):
    current_files = set(os.listdir(folder_path))
    processed_files = {file for files in inverted_index.values() for file in files}
    new_files = current_files - processed_files
    
    new_files_count = 0  # Contador para archivos nuevos
    
    # Usar ThreadPoolExecutor para paralelizar el procesamiento de PDFs
    with ThreadPoolExecutor(max_workers=4) as executor:
        for filename in new_files:
            if filename.endswith('.pdf'):
                pdf_path = os.path.join(folder_path, filename)
                new_files_count += 1
                executor.submit(process_pdf, pdf_path, inverted_index, output_path)
    
    if new_files_count > 0:
        print(f'{new_files_count} nuevos archivos detectados y procesados.')
    else:
        print('No se detectaron nuevos archivos.')

# Función para ejecutar el daemon
def run(folder_path, output_path):
    inverted_index = load_inverted_index(output_path)
    while True:
        check_for_new_files(folder_path, inverted_index, output_path)
        time.sleep(60)  # Esperar 60 segundos antes de ejecutar de nuevo

# Función para iniciar el daemon en segundo plano
def start_daemon(folder_path, output_path):
    thread = threading.Thread(target=run, args=(folder_path, output_path), daemon=True)
    thread.start()
