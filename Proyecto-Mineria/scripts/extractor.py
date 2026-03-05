#!/usr/bin/env python3
import os
import requests
import time
import logging
import json
from dotenv import load_dotenv
from pathlib import Path

# ==============================
# Configuración
# ==============================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SuperheroExtractor:

    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = os.getenv('SUPERHERO_BASE_URL')

    # ==============================
    # Obtener héroe desde API
    # ==============================
    def obtener_heroe_por_id(self, hero_id):

        try:
            url = f"{self.base_url}/{self.api_key}/{hero_id}"

            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("response") == "error":
                return None

            return data

        except Exception as e:
            logger.error(f"Error con ID {hero_id}: {e}")
            return None


    # ==============================
    # Ejecutar extracción
    # ==============================
    def ejecutar_extraccion(self, limite=731):

        heroes = []

        for hero_id in range(1, limite + 1):

            hero = self.obtener_heroe_por_id(hero_id)

            if hero:
                heroes.append(hero)
                logger.info(f"Héroe {hero_id} extraído")

            time.sleep(0.2)

        # guardar RAW
        ruta_salida = DATA_DIR / "superheroes_raw.json"

        with open(ruta_salida, "w", encoding="utf-8") as f:
            json.dump(heroes, f, indent=4, ensure_ascii=False)

        logger.info(f"Extracción completada. {len(heroes)} héroes guardados en RAW.")


# ==============================
# Main
# ==============================
if __name__ == "__main__":

    extractor = SuperheroExtractor()
    extractor.ejecutar_extraccion(731)