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
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SuperheroTransformador:
    def __init__(self, input_json=DATA_DIR / "superheroes_raw.json"):
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
            powerstats = hero.get("powerstats", {}) or {}
            biography = hero.get("biography", {}) or {}
            appearance = hero.get("appearance", {}) or {}

            altura = appearance.get("height", [])
            peso = appearance.get("weight", [])

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
                "altura": ", ".join(altura) if isinstance(altura, list) and altura else altura,
                "peso": ", ".join(peso) if isinstance(peso, list) and peso else peso,
                "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        self.df = pd.DataFrame(heroes)
        logger.info(f"📂 Datos cargados: {len(self.df)} registros desde {self.input_json}")
        return self

    def limpiar_datos(self):
        if self.df is None:
            raise ValueError("Primero debes cargar los datos con cargar_datos().")

        filas_antes = len(self.df)

        self.df.drop_duplicates(subset=["id"], inplace=True)

        cols_texto = [
            "nombre", "nombre_completo", "editor",
            "alineacion", "genero", "raza", "altura", "peso"
        ]

        valores_invalidos = ["", " ", "-", "null", "None", "nan", "N/A", None]

        for col in cols_texto:
            if col in self.df.columns:
                self.df[col] = self.df[col].replace(valores_invalidos, pd.NA)
                self.df[col] = self.df[col].fillna("Desconocido")

        filas_despues = len(self.df)

        logger.info(
            f"🧹 Limpieza completada: {filas_antes - filas_despues} duplicados eliminados, "
            f"{filas_despues} registros restantes"
        )
        return self

    def normalizar_tipos(self):
        if self.df is None:
            raise ValueError("Primero debes cargar los datos con cargar_datos().")

        cols_numericas = [
            "id", "inteligencia", "fuerza",
            "velocidad", "durabilidad", "poder", "combate"
        ]

        for col in cols_numericas:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

        self.df[cols_numericas] = self.df[cols_numericas].fillna(0)

        logger.info("🔧 Tipos de datos normalizados")
        return self

    def enriquecer_datos(self):
        if self.df is None:
            raise ValueError("Primero debes cargar los datos con cargar_datos().")

        numeric_cols = [
            "inteligencia", "fuerza",
            "velocidad", "durabilidad", "poder", "combate"
        ]

        self.df["promedio_poder"] = self.df[numeric_cols].mean(axis=1).round(2)
        self.df["ranking"] = self.df["promedio_poder"].rank(ascending=False, method="min")

        logger.info("✨ Datos enriquecidos con columnas calculadas")
        return self

    def guardar_datos(self, output_csv=DATA_DIR / "superheroes.csv"):
        if self.df is None:
            raise ValueError("No hay datos para guardar.")

        cols_int = [
            "id", "inteligencia", "fuerza",
            "velocidad", "durabilidad", "poder", "combate"
        ]

        for col in cols_int:
            self.df[col] = self.df[col].fillna(0).astype(int)

        self.df["promedio_poder"] = self.df["promedio_poder"].fillna(0).astype(float)
        self.df["ranking"] = self.df["ranking"].fillna(0).astype(float)

        os.makedirs(DATA_DIR, exist_ok=True)
        self.df.to_csv(output_csv, index=False, encoding="utf-8")

        logger.info(f"💾 Datos transformados guardados en {output_csv}")

        top10_path = DATA_DIR / "top10_superheroes.csv"
        self.df.sort_values(by="promedio_poder", ascending=False).head(10).to_csv(
            top10_path, index=False, encoding="utf-8"
        )
        logger.info(f"💾 Top 10 guardado en {top10_path}")

        return self.df

    def mostrar_resumen(self):
        if self.df is None:
            raise ValueError("No hay datos cargados para resumir.")

        print("\n" + "=" * 60)
        print("ESTADÍSTICAS DEL DATASET TRANSFORMADO")
        print("=" * 60)

        cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
        print(self.df[cols].describe().round(2).to_string())

        print("\nValores nulos por columna:")
        print(self.df.isnull().sum().to_string())

        print("\nTop editoriales:")
        print(self.df["editor"].value_counts().head(10).to_string())

        print("=" * 60)


if __name__ == "__main__":
    try:
        transformador = SuperheroTransformador()
        df = (
            transformador
            .cargar_datos()
            .limpiar_datos()
            .normalizar_tipos()
            .enriquecer_datos()
            .guardar_datos()
        )
        transformador.mostrar_resumen()

    except FileNotFoundError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"❌ Error fatal en transformación: {str(e)}")
        raise