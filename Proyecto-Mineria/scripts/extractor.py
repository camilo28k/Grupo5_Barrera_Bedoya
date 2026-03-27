#!/usr/bin/env python3
"""
extractor.py - Fase Extract del pipeline ETL
Consume la API de SuperHero y guarda los datos en JSON.
"""

import os
import requests
import json
import time
from dotenv import load_dotenv
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BASE_DIR / 'logs' / 'etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SuperheroExtractor:
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.base_url = os.getenv("SUPERHERO_BASE_URL")

        if not self.api_key:
            raise ValueError("API_KEY no configurada. Verifica tu archivo .env")

    def extraer_superheroe(self, hero_id):
        try:
            response = requests.get(
                f"{self.base_url}/{self.api_key}/{hero_id}",
                timeout=10
            )
            data = response.json()

            if not response.ok or data.get("response") == "error":
                logger.warning(f"⚠️ No se pudo obtener héroe ID {hero_id}")
                return None

            return data

        except requests.exceptions.Timeout:
            logger.error(f"Timeout al consultar héroe {hero_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red con héroe {hero_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado con héroe {hero_id}: {str(e)}")
            return None

    def ejecutar_extraccion(self, limite=731):
        datos_extraidos = []

        logger.info(f"Iniciando extracción para {limite} héroes...")

        for hero_id in range(1, limite + 1):
            raw_data = self.extraer_superheroe(hero_id)

            if raw_data:
                datos_extraidos.append(raw_data)
                logger.info(f"✅ Héroe {hero_id}: {raw_data.get('name', 'N/A')}")

            time.sleep(0.2)

        logger.info(f"Extracción completada: {len(datos_extraidos)}/{limite} héroes exitosos")
        return datos_extraidos


if __name__ == "__main__":
    try:
        extractor = SuperheroExtractor()
        datos = extractor.ejecutar_extraccion(731)

        if not datos:
            logger.error("No se extrajeron datos. Verifica la API key y la conexión.")
            raise SystemExit(1)

        os.makedirs(DATA_DIR, exist_ok=True)

        with open(DATA_DIR / 'superheroes_raw.json', 'w', encoding='utf-8') as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)

        logger.info("📁 Datos guardados en data/superheroes_raw.json")

    except Exception as e:
        logger.error(f"Error fatal en extracción: {str(e)}")
        raise