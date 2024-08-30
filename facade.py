import os
import subprocess
import json
from typing import List, Dict, Any
from cassandra.cluster import Cluster
from crawler_daemon import start_daemon

class BuscadorFacade:
    def __init__(self, procesar_consulta_script: str, ranking_script: str, ruta_documentos: str):
        self.procesar_consulta_script = procesar_consulta_script
        self.ranking_script = ranking_script
        self.ruta_documentos = ruta_documentos

        # Ruta del índice invertido
        self.output_path = '/Users/renzo/Desktop/seminario/Buscador/indice_invertido_con_stopwords_normalizados_github.json'

        # Iniciar el daemon del crawler en segundo plano
        start_daemon(self.ruta_documentos, self.output_path)

        # Configuración de Cassandra
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('indice_invertido_keyspace')

    def buscar_documentos(self, query: str) -> List[Dict[str, Any]]:
        try:
            # Ejecutar el script de procesamiento de consulta
            consulta_resultado = subprocess.run(
                ['python', self.procesar_consulta_script, query],
                capture_output=True,
                text=True,
                check=True
            )
            print("Salida de procesar_consulta.py (bruta):", consulta_resultado.stdout)  # Depuración
            
            if not consulta_resultado.stdout.strip():
                print("Error: La salida de procesar_consulta.py está vacía.")
                return []

            try:
                terminos_clave = json.loads(consulta_resultado.stdout)
            except json.JSONDecodeError as e:
                print("Error al decodificar JSON:", e)
                print("Contenido de la salida:", consulta_resultado.stdout)  # Depuración adicional
                return []

            print("Términos clave generados:", terminos_clave)  # Depuración
            
            if not terminos_clave:
                print("Error: El JSON de términos clave está vacío.")
                return []

            # Crear un archivo temporal con los términos clave
            with open('terminos_clave.json', 'w') as f:
                json.dump(terminos_clave, f)

            # Buscar documentos en Cassandra
            documentos = self.buscar_en_cassandra(terminos_clave)
            print("Documentos encontrados en Cassandra:", documentos)  # Depuración

            if not documentos:
                print("No se encontraron documentos para los términos clave.")
                return []

            # Ejecutar el script de ranking con los documentos encontrados
            ranking_resultado = subprocess.run(
                ['python', self.ranking_script, json.dumps(documentos), self.ruta_documentos],
                capture_output=True,
                text=True,
                check=True
            )
            print("Salida de ranking.py:", ranking_resultado.stdout)  # Depuración
            if not ranking_resultado.stdout.strip():
                print("Error: La salida de ranking.py está vacía.")
                return []
            
            resultados = json.loads(ranking_resultado.stdout)
            print("Resultados después del ranking:", resultados)  # Depuración
            
            # Añadir el nombre del documento al resultado
            for resultado in resultados:
                resultado['doc_nombre'] = f"{resultado['doc_id']}.pdf"

            return resultados

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error al ejecutar los scripts: {e.stderr}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Error al decodificar JSON: {e.msg}")
        except Exception as e:
            raise RuntimeError(f"Error al buscar documentos: {e}")

    def buscar_en_cassandra(self, terminos_clave: List[str]) -> List[str]:
        documentos = []
        for termino in terminos_clave:
            print(f"Buscando en Cassandra por el término: {termino}")  # Depuración
            resultados = self.session.execute(
                "SELECT files FROM indice_invertido_table WHERE word = %s", 
                [termino]
            )
            for row in resultados:
                print(f"Resultados encontrados para {termino}: {row.files}")  # Depuración
                documentos.extend(row.files)
        documentos = list(set(documentos))  # Eliminar duplicados
        print("Documentos finales encontrados:", documentos)  # Depuración
        return documentos

    def shutdown(self):
        self.session.shutdown()
        self.cluster.shutdown()
