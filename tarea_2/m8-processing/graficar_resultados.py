#!/usr/bin/env python3
# script extra, para graficar los resultados de los CSV generados por el procesamiento de datos con PIG y que se vea mas bonito
import os
import pandas as pd
import matplotlib.pyplot as plt
import re
from pathlib import Path

def encontrar_archivos_csv(directorio):
    """Busca archivos CSV que no empiecen con 'encabezado'"""
    archivos = []
    for root, _, files in os.walk(directorio):
        for file in files:
            if file.endswith('.csv') and not file.startswith('encabezado'):
                archivos.append(os.path.join(root, file))
    return archivos

def crear_grafico(df, tipo, directorio_salida):
    """Crea gráficos según el tipo de archivo CSV"""
    plt.figure(figsize=(12, 8))
    
    if tipo == 'calles_con_mas_accidentes':
        # top 10 calles con más accidentes
        top_10 = df.sort_values('cantidad_accidentes', ascending=False).head(10)
        top_10.plot(kind='bar', x='calle', y='cantidad_accidentes', color='crimson')
        plt.title('Top 10 Calles con Más Accidentes', fontsize=16)
        plt.xlabel('Calle', fontsize=14)
        plt.ylabel('Cantidad de Accidentes', fontsize=14)
        
    elif tipo == 'calles_con_mas_alertas':
        # top 10 calles con más alertas
        top_10 = df.sort_values('cantidad_alertas', ascending=False).head(10)
        top_10.plot(kind='bar', x='calle', y='cantidad_alertas', color='orange')
        plt.title('Top 10 Calles con Más Alertas', fontsize=16)
        plt.xlabel('Calle', fontsize=14)
        plt.ylabel('Cantidad de Alertas', fontsize=14)
        
    elif tipo == 'comunas_con_mas_accidentes':
        # top 5 comunas con más accidentes
        top_5 = df.sort_values('cantidad_accidentes', ascending=False).head(5)
        top_5.plot(kind='bar', x='comuna', y='cantidad_accidentes', color='darkblue')
        plt.title('Top 5 Comunas con Más Accidentes', fontsize=16)
        plt.xlabel('Comuna', fontsize=14)
        plt.ylabel('Cantidad de Accidentes', fontsize=14)
        
    elif tipo == 'comunas_con_mas_alertas':
        # top 10 comunas con más alertas
        top_10 = df.sort_values('cantidad_alertas', ascending=False).head(10)
        top_10.plot(kind='bar', x='comuna', y='cantidad_alertas', color='purple')
        plt.title('Top 10 Comunas con Más Alertas', fontsize=16)
        plt.xlabel('Comuna', fontsize=14)
        plt.ylabel('Cantidad de Alertas', fontsize=14)
        
    elif tipo == 'horas_pico':
        # distribución de alertas por hora
        df = df.sort_values('hora')
        plt.plot(df['hora'], df['cantidad_alertas'], marker='o', color='green', linewidth=2)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.title('Distribución de Alertas por Hora del Día', fontsize=16)
        plt.xlabel('Hora', fontsize=14)
        plt.ylabel('Cantidad de Alertas', fontsize=14)
        plt.xticks(df['hora'])
        
    elif tipo == 'tipos_alerta_frecuencia':
        #frecuencia de tipos de alerta
        df = df.sort_values('cantidad', ascending=False)
        df.plot(kind='bar', x='tipo_alerta', y='cantidad', color='teal')
        plt.title('Frecuencia de Tipos de Alerta', fontsize=16)
        plt.xlabel('Tipo de Alerta', fontsize=14)
        plt.ylabel('Cantidad', fontsize=14)
        
    elif tipo == 'atascos_por_ciudad_largo':
        #top 10 ciudades con mayor largo total de atascos
        top_10 = df.sort_values('largo_total', ascending=False).head(10)
        top_10.plot(kind='bar', x='ciudad', y='largo_total', color='royalblue')
        plt.title('Top 10 Comunas con Mayor Largo Total de Atascos', fontsize=16)
        plt.xlabel('Comuna', fontsize=14)
        plt.ylabel('Largo Total (m)', fontsize=14)
        
    elif tipo == 'atascos_por_ciudad_numero':
        # top 10 ciudades con mayor número de atascos
        top_10 = df.sort_values('num_atascos', ascending=False).head(10)
        top_10.plot(kind='bar', x='ciudad', y='num_atascos', color='darkorange')
        plt.title('Top 10 Comunas con Mayor Número de Atascos', fontsize=16)
        plt.xlabel('Comuna', fontsize=14)
        plt.ylabel('Número de Atascos', fontsize=14)
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    # guardar gráfico
    ruta_salida = os.path.join(directorio_salida, f"{tipo}.png")
    plt.savefig(ruta_salida, dpi=300)
    plt.close()
    print(f"Gráfico guardado en: {ruta_salida}")

def main():
    # configuración
    directorio_resultados = "/app/results"
    
    # encontrar el directorio de ejecución más reciente
    patron = re.compile(r'ejecucion_\d{8}_\d{6}$')
    directorios = [d for d in os.listdir(directorio_resultados) 
                  if os.path.isdir(os.path.join(directorio_resultados, d)) and 
                  patron.match(d)]
    
    if not directorios:
        print("No se encontraron directorios de ejecución.")
        return
    
    # ordenar por fecha (más reciente primero)
    directorios.sort(reverse=True)
    directorio_actual = os.path.join(directorio_resultados, directorios[0])
    print(f"Usando el directorio: {directorio_actual}")
    
    # crear directorio para gráficos
    directorio_graficos = os.path.join(directorio_actual, 'graficos')
    os.makedirs(directorio_graficos, exist_ok=True)
    
    # Buscar archivos CSV
    archivos_csv = encontrar_archivos_csv(directorio_actual)
    
    if not archivos_csv:
        print("No se encontraron archivos CSV para analizar.")
        return
    
    # Mapeo de nombres de archivo a tipos de gráfico
    tipo_mapa = {
        'calles_con_mas_accidentes': 'calles_con_mas_accidentes',
        'calles_con_mas_alertas': 'calles_con_mas_alertas',
        'comunas_con_mas_accidentes': 'comunas_con_mas_accidentes',
        'comunas_con_mas_alertas': 'comunas_con_mas_alertas',
        'horas_pico': 'horas_pico',
        'tipos_alerta_frecuencia': 'tipos_alerta_frecuencia'
    }
    
    # Procesar cada archivo CSV
    for archivo in archivos_csv:
        try:
            nombre_base = Path(archivo).stem.lower()
            
            # Caso especial para atascos_por_ciudad
            if 'atascos_por_ciudad' in nombre_base:
                print(f"Procesando {archivo} como atascos_por_ciudad")
                df = pd.read_csv(archivo)
                
                # Generar dos gráficos para este archivo
                crear_grafico(df, 'atascos_por_ciudad_largo', directorio_graficos)
                crear_grafico(df, 'atascos_por_ciudad_numero', directorio_graficos)
                continue
            
            # Determinar el tipo de gráfico a generar (para otros archivos)
            tipo_grafico = None
            for clave, valor in tipo_mapa.items():
                if clave in nombre_base:
                    tipo_grafico = valor
                    break
            
            if tipo_grafico:
                print(f"Procesando {archivo} como {tipo_grafico}")
                df = pd.read_csv(archivo)
                crear_grafico(df, tipo_grafico, directorio_graficos)
            else:
                print(f"No se reconoce el tipo de archivo: {nombre_base}")
                
        except Exception as e:
            print(f"Error al procesar {archivo}: {e}")
    
    print(f"Se han generado gráficos en: {directorio_graficos}")

if __name__ == "__main__":
    main()