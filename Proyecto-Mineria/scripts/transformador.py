#!/usr/bin/env python3
import json
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    # Verificar que exista archivo raw
    if not os.path.exists('data/superheroes_raw.json'):
        logger.error("❌ No existe data/superheroes_raw.json. Ejecuta primero extractor.py")
        return

    # Leer JSON crudo
    with open('data/superheroes_raw.json', 'r') as f:
        data = json.load(f)

    # Convertir a DataFrame
    df = pd.DataFrame(data)

    # Columnas numéricas
    numeric_cols = [
        'inteligencia',
        'fuerza',
        'velocidad',
        'durabilidad',
        'poder',
        'combate'
    ]

    # Convertir a numérico
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Crear nueva métrica: promedio total de habilidades
    df['promedio_poder'] = df[numeric_cols].mean(axis=1)

    # Ordenar por promedio
    df = df.sort_values(by='promedio_poder', ascending=False)

    # Crear carpeta data si no existe
    os.makedirs("data", exist_ok=True)

    # Guardar archivo transformado
    df.to_csv('data/superheroes_transformado.csv', index=False)

    logger.info("✅ Datos transformados guardados en data/superheroes_transformado.csv")
    print("\nDatos transformados correctamente.\n")
    print(df[['nombre', 'promedio_poder']])

if __name__ == "__main__":
    main()