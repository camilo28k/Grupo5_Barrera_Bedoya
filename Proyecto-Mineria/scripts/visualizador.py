#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging
import os

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    # Verificar que exista archivo transformado
    if not os.path.exists('data/superheroes_transformado.csv'):
        logger.error("❌ No existe data/superheroes_transformado.csv. Ejecuta primero transformador.py")
        return

    # Cargar datos transformados
    df = pd.read_csv('data/superheroes_transformado.csv')

    # Columnas numéricas
    numeric_cols = [
        'inteligencia', 'fuerza', 'velocidad',
        'durabilidad', 'poder', 'combate'
    ]

    # Convertir columnas numéricas
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Crear figura con múltiples gráficas
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Análisis de Superhéroes', fontsize=16, fontweight='bold')

    # ===== Gráfica 1: Inteligencia =====
    ax1 = axes[0, 0]
    ax1.bar(df['nombre'], df['inteligencia'])
    ax1.set_title('Nivel de Inteligencia')
    ax1.set_ylabel('Inteligencia')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(axis='y', alpha=0.3)

    # ===== Gráfica 2: Fuerza =====
    ax2 = axes[0, 1]
    ax2.bar(df['nombre'], df['fuerza'])
    ax2.set_title('Nivel de Fuerza')
    ax2.set_ylabel('Fuerza')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(axis='y', alpha=0.3)

    # ===== Gráfica 3: Velocidad =====
    ax3 = axes[1, 0]
    ax3.scatter(df['nombre'], df['velocidad'], s=200)
    ax3.set_title('Velocidad')
    ax3.set_ylabel('Velocidad')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(alpha=0.3)

    # ===== Gráfica 4: Poder vs Combate =====
    ax4 = axes[1, 1]
    x = np.arange(len(df))
    width = 0.35

    ax4.bar(x - width/2, df['poder'], width, label='Poder')
    ax4.bar(x + width/2, df['combate'], width, label='Combate')

    ax4.set_title('Poder vs Combate')
    ax4.set_ylabel('Nivel')
    ax4.set_xticks(x)
    ax4.set_xticklabels(df['nombre'], rotation=45)
    ax4.legend()
    ax4.grid(axis='y', alpha=0.3)

    plt.tight_layout()

    # Crear carpeta data si no existe
    os.makedirs("data", exist_ok=True)

    plt.savefig('data/superhero_analysis.png', dpi=300, bbox_inches='tight')
    logger.info("✅ Gráficas guardadas en data/superhero_analysis.png")

    plt.show()


if __name__ == "__main__":
    main()