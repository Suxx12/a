import json
import matplotlib.pyplot as plt
from collections import Counter
import numpy as np

# Leer el archivo JSON con la distribución Zipf
try:
    with open("/app/uuids_zipf_2.json", 'r') as f:
        uuids_zipf = json.load(f)
except FileNotFoundError:
    # Intentar leer desde una ruta relativa si la anterior falla
    with open("uuids_zipf_2.json", 'r') as f:
        uuids_zipf = json.load(f)

# Extraer solo los UUIDs para contar
uuids_solo = [item["uuid"] for item in uuids_zipf]

# Contar frecuencia de cada UUID
contador = Counter(uuids_solo)

# Obtener los 50 UUIDs más frecuentes para una mejor visualización
top_uuids = contador.most_common(50)
uuids = [f"{uuid[:8]}..." for uuid, _ in top_uuids]  # Truncar UUIDs largos
frecuencias = [count for _, count in top_uuids]

# Preparar gráficos
plt.figure(figsize=(15, 10))

# Gráfico de barras
plt.subplot(2, 1, 1)
plt.bar(range(len(uuids)), frecuencias, color='skyblue')
plt.xlabel('UUID (primeros 8 caracteres)')
plt.ylabel('Frecuencia')
plt.title('Frecuencia de los 50 UUIDs más comunes')
plt.xticks(range(len(uuids)), uuids, rotation=90)
plt.tight_layout()

# Gráfico de clasificación por orden y curva logarítmica para verificar ley de Zipf
plt.subplot(2, 1, 2)
# Ordenar todas las frecuencias de mayor a menor
all_freqs = sorted(contador.values(), reverse=True)
ranks = np.arange(1, len(all_freqs) + 1)

plt.loglog(ranks, all_freqs, 'o', markersize=3, alpha=0.5)
plt.xlabel('Rango (escala logarítmica)')
plt.ylabel('Frecuencia (escala logarítmica)')
plt.title('Distribución de frecuencias - Verificación de Ley de Zipf')
plt.grid(True, which="both", ls="-", alpha=0.2)

# Añadir estadísticas
plt.figtext(0.5, 0.01, 
           f"Total de UUIDs únicos: {len(contador)}\n"
           f"UUID más frecuente aparece {max(contador.values())} veces\n"
           f"UUID menos frecuente aparece {min(contador.values())} veces", 
           ha="center", fontsize=12, bbox={"facecolor":"orange", "alpha":0.2, "pad":5})

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.suptitle('Análisis de Distribución Zipf de UUIDs', fontsize=16)
plt.subplots_adjust(bottom=0.15)

# Guardar figura
plt.savefig('distribucion_zipf_2.png', dpi=300, bbox_inches='tight')

# Mostrar gráfico
plt.show()

# Imprimir algunas estadísticas adicionales
print(f"Total de UUIDs en el archivo: {len(uuids_zipf)}")
print(f"Número de UUIDs únicos: {len(contador)}")
print(f"Top 10 UUIDs más frecuentes:")
for i, (uuid, count) in enumerate(contador.most_common(10), 1):
    print(f"{i}. UUID: {uuid[:8]}... - Aparece {count} veces")