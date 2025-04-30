import json
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from collections import Counter
import argparse

def plot_uuid_distribution(uuids, output_path):
    """Genera y guarda un gráfico de la distribución de UUIDs basado en su frecuencia."""
    try:
        if len(uuids) == 0:
            print("Error: No se encontraron UUIDs para analizar")
            return False
        
        # Contar frecuencia de cada UUID
        uuid_counter = Counter(uuids)
        
        # Obtener frecuencias
        frequencies = list(uuid_counter.values())
        
        # Calcular estadísticas
        mean = np.mean(frequencies)
        std = np.std(frequencies)
        max_freq = max(frequencies)
        min_freq = min(frequencies)
        
        # Crear figura para la distribución de frecuencias
        plt.figure(figsize=(12, 7))
        
        # Histograma de frecuencias
        plt.hist(frequencies, bins=30, alpha=0.7, color='blue', 
                 edgecolor='black', label='Frecuencia de UUIDs')
        
        # Si hay suficientes datos diferentes, intentar ajustar una curva normal
        if len(set(frequencies)) > 5:
            xmin, xmax = plt.xlim()
            x = np.linspace(xmin, xmax, 100)
            p = 1/(std * np.sqrt(2 * np.pi)) * np.exp(-(x - mean)**2 / (2 * std**2))
            plt.plot(x, p * len(frequencies) * (xmax-xmin)/30, 'r--', 
                     linewidth=2, label='Ajuste normal')
        
        # Añadir estadísticas al gráfico
        plt.title('Distribución de Frecuencias de UUIDs')
        plt.xlabel('Frecuencia (veces que se repite cada UUID)')
        plt.ylabel('Cantidad de UUIDs')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Añadir cuadro con estadísticas
        stats_text = (
            f"Total UUIDs únicos: {len(uuid_counter)}\n"
            f"Total menciones: {len(uuids)}\n"
            f"Frecuencia media: {mean:.2f}\n"
            f"Desviación estándar: {std:.2f}\n"
            f"Frecuencia máxima: {max_freq}\n"
            f"Frecuencia mínima: {min_freq}"
        )
        plt.figtext(0.70, 0.15, stats_text, bbox=dict(facecolor='white', alpha=0.8))
        
        # Añadir marca de tiempo
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        plt.figtext(0.5, 0.01, f"Generado: {timestamp}", ha="center", fontsize=9)
        
        # Guardar el gráfico principal
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        
        print(f"Gráfico guardado en: {output_path}")
        return True
        
    except Exception as e:
        print(f"Error al generar el gráfico: {str(e)}")
        return False

def extract_uuids_from_json(json_file):
    """Extrae los UUIDs de un archivo JSON que contiene una lista de objetos con campo 'uuid'."""
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Verificar si es una lista de objetos o una lista simple
        if isinstance(data, list):
            if len(data) > 0:
                # Si es una lista de objetos con campo uuid
                if isinstance(data[0], dict) and 'uuid' in data[0]:
                    return [item['uuid'] for item in data]
                # Si es una lista simple de UUIDs
                else:
                    return data
        
        print(f"Error: Formato de JSON no compatible en {json_file}")
        return []
    
    except Exception as e:
        print(f"Error al procesar el archivo JSON {json_file}: {str(e)}")
        return []

def main():
    """Función principal que procesa argumentos y genera el gráfico."""
    parser = argparse.ArgumentParser(description='Visualizador de distribución de UUIDs')
    parser.add_argument('json_file', help='Archivo JSON con la lista de UUIDs')
    parser.add_argument('--output', '-o', help='Ruta del archivo de salida del gráfico', 
                        default='distribucion_uuids.png')
    
    args = parser.parse_args()
    
    # Verificar que el archivo existe
    if not os.path.exists(args.json_file):
        print(f"Error: El archivo {args.json_file} no existe")
        return 1
    
    # Extraer UUIDs del archivo JSON
    uuids = extract_uuids_from_json(args.json_file)
    
    if not uuids:
        print("No se encontraron UUIDs para analizar")
        return 1
    
    # Generar el gráfico
    if plot_uuid_distribution(uuids, args.output):
        print("Proceso completado exitosamente")
        return 0
    else:
        print("Error al generar el gráfico")
        return 1

if __name__ == "__main__":
    sys.exit(main())