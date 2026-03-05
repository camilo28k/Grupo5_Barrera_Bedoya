#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import logging
from pathlib import Path

# ==============================
# Rutas del proyecto
# ==============================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
GRAFICAS_DIR = BASE_DIR / "graficas"

GRAFICAS_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==============================
# Gráfica Top 10
# ==============================

def graficar_top10(df, columna, titulo):

    top10 = df.sort_values(by=columna, ascending=False).head(10)

    plt.figure(figsize=(10,6))
    plt.barh(top10['nombre'], top10[columna])
    plt.gca().invert_yaxis()

    plt.title(f"Top 10 por {titulo}", fontsize=14, fontweight='bold')
    plt.xlabel(titulo)
    plt.ylabel("Héroe")

    plt.tight_layout()

    ruta = GRAFICAS_DIR / f"top10_{columna}.png"
    plt.savefig(ruta)
    plt.close()

    logger.info(f"Gráfica guardada: {ruta.name}")


# ==============================
# Gráfica de torta
# ==============================

def graficar_torta_alineacion(df):

    if 'alineacion' not in df.columns:
        logger.warning("No existe columna alineacion")
        return

    alineacion_counts = df['alineacion'].value_counts()

    plt.figure(figsize=(6,6))

    plt.pie(
        alineacion_counts,
        labels=alineacion_counts.index,
        autopct='%1.1f%%',
        startangle=140
    )

    plt.title("Distribución por Alineación")
    plt.tight_layout()

    ruta = GRAFICAS_DIR / "alineacion_torta.png"
    plt.savefig(ruta)
    plt.close()

    logger.info(f"Gráfica guardada: {ruta.name}")


# ==============================
# MAIN
# ==============================

def main():

    archivo = DATA_DIR / "superheroes.csv"

    if not archivo.exists():
        logger.error("Primero ejecuta transformador.py")
        return

    df = pd.read_csv(archivo)

    columnas_numericas = [
        'inteligencia',
        'fuerza',
        'velocidad',
        'durabilidad',
        'poder',
        'combate'
    ]

    # limpieza de datos
    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.fillna(0)

    # ==============================
    # Gráficas de poderes
    # ==============================

    for col in columnas_numericas:
        graficar_top10(df, col, col.capitalize())

    # ==============================
    # Gráfica de alineación
    # ==============================

    graficar_torta_alineacion(df)

    logger.info("Visualización finalizada correctamente")


if __name__ == "__main__":
    main()