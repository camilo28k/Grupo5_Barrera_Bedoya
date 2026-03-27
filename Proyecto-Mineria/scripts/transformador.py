#!/usr/bin/env python3
"""
transformador.py - Fase Transform del pipeline ETL
Limpia, normaliza y enriquece los datos extraídos de superhéroes.
"""

import pandas as pd
import json
import os
import logging
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BASE_DIR / 'logs' / 'etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SuperheroTransformador:
    def __init__(self, input_json=DATA_DIR / 'superheroes_raw.json'):
        self.input_json = input_json
        self.df = None

    def cargar_datos(self):
        if not os.path.exists(self.input_json):
            raise FileNotFoundError(
                f"Archivo {self.input_json} no encontrado. "
                "Ejecuta primero scripts/extractor.py"
            )

        with open(self.input_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        heroes = []

        for hero in data:
            powerstats = hero.get("powerstats", {})
            biography = hero.get("biography", {})
            appearance = hero.get("appearance", {})

            heroes.append({
                "id": hero.get("id"),
                "nombre": hero.get("name"),
                "inteligencia": powerstats.get("intelligence"),
                "fuerza": powerstats.get("strength"),
                "velocidad": powerstats.get("speed"),
                "durabilidad": powerstats.get("durability"),
                "poder": powerstats.get("power"),
                "combate": powerstats.get("combat"),
                "nombre_completo": biography.get("full-name"),
                "editor": biography.get("publisher"),
                "alineacion": biography.get("alignment"),
                "genero": appearance.get("gender"),
                "raza": appearance.get("race"),
                "altura": ", ".join(appearance.get("height", [])) if appearance.get("height") else "",
                "peso": ", ".join(appearance.get("weight", [])) if appearance.get("weight") else "",
                "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        self.df = pd.DataFrame(heroes)
        logger.info(f"📂 Datos cargados: {len(self.df)} registros desde {self.input_json}")
        return self

    def limpiar_datos(self):
        filas_antes = len(self.df)
        self.df.drop_duplicates(subset=["id"], inplace=True)
        self.df.fillna({
            "nombre": "N/A",
            "nombre_completo": "N/A",
            "editor": "N/A",
            "alineacion": "N/A",
            "genero": "N/A",
            "raza": "N/A",
            "altura": "N/A",
            "peso": "N/A"
        }, inplace=True)
        filas_despues = len(self.df)

        logger.info(
            f"🧹 Limpieza: {filas_antes - filas_despues} duplicados eliminados, "
            f"{filas_despues} registros restantes"
        )
        return self

    def normalizar_tipos(self):
        cols_numericas = [
            "id", "inteligencia", "fuerza",
            "velocidad", "durabilidad", "poder", "combate"
        ]

        for col in cols_numericas:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        logger.info("🔧 Tipos de datos normalizados")
        return self

    def enriquecer_datos(self):
        numeric_cols = [
            "inteligencia", "fuerza",
            "velocidad", "durabilidad", "poder", "combate"
        ]

        self.df["promedio_poder"] = self.df[numeric_cols].mean(axis=1).round(2)
        self.df["ranking"] = self.df["promedio_poder"].rank(ascending=False, method="min")

        logger.info("✨ Datos enriquecidos con columnas calculadas")
        return self

    def guardar_datos(self, output_csv=DATA_DIR / 'superheroes.csv'):
        self.df = self.df.fillna(0)

        cols_int = [
            "id", "inteligencia", "fuerza",
            "velocidad", "durabilidad", "poder", "combate"
        ]
        for col in cols_int:
            self.df[col] = self.df[col].astype(int)

        self.df["ranking"] = self.df["ranking"].astype(float)
        self.df["promedio_poder"] = self.df["promedio_poder"].astype(float)

        os.makedirs(DATA_DIR, exist_ok=True)
        self.df.to_csv(output_csv, index=False)
        logger.info(f"💾 Datos transformados guardados en {output_csv}")

        top10_path = DATA_DIR / "top10_superheroes.csv"
        self.df.sort_values(by="promedio_poder", ascending=False).head(10).to_csv(top10_path, index=False)
        logger.info(f"💾 Top 10 guardado en {top10_path}")

        return self.df

    def mostrar_resumen(self):
        print("\n" + "=" * 60)
        print("ESTADÍSTICAS DEL DATASET TRANSFORMADO")
        print("=" * 60)
        cols = ['inteligencia', 'fuerza', 'velocidad', 'durabilidad', 'poder', 'combate']
        print(self.df[cols].describe().round(2).to_string())
        print("\nTop editoriales:")
        print(self.df['editor'].value_counts().head(10).to_string())
        print("=" * 60)


if __name__ == "__main__":
    try:
        transformador = SuperheroTransformador()
        df = (transformador
              .cargar_datos()
              .limpiar_datos()
              .normalizar_tipos()
              .enriquecer_datos()
              .guardar_datos())
        transformador.mostrar_resumen()

    except FileNotFoundError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Error fatal en transformación: {str(e)}")
        raise