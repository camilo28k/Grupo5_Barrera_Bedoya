#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SuperheroExtractor:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = os.getenv('SUPERHERO_BASE_URL')
        self.heroes = os.getenv('HEROES').split(',')

        if not self.api_key:
            raise ValueError("API_KEY no configurada en .env")

    def buscar_heroe(self, nombre):
        """Busca un héroe por nombre"""
        try:
            url = f"{self.base_url}/{self.api_key}/search/{nombre.strip()}"

            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("response") == "error":
                logger.error(f"❌ Error en API para {nombre}: {data.get('error')}")
                return None

            logger.info(f"✅ Datos extraídos para {nombre}")
            return data

        except Exception as e:
            logger.error(f"❌ Error extrayendo datos para {nombre}: {str(e)}")
            return None

    def procesar_respuesta(self, response_data):
        """Procesa la respuesta JSON a formato estructurado"""
        try:
            results = response_data.get("results", [])

            if not results:
                return None

            hero = results[0]  # Tomamos el primer resultado

            powerstats = hero.get("powerstats", {})
            biography = hero.get("biography", {})
            appearance = hero.get("appearance", {})

            return {
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
                "altura": appearance.get("height")[1] if appearance.get("height") else None,
                "peso": appearance.get("weight")[1] if appearance.get("weight") else None,
                "fecha_extraccion": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error procesando respuesta: {str(e)}")
            return None

    def ejecutar_extraccion(self):
        """Ejecuta la extracción para todos los héroes"""
        datos_extraidos = []

        logger.info(f"Iniciando extracción para {len(self.heroes)} héroes...")

        for hero in self.heroes:
            response = self.buscar_heroe(hero)
            if response:
                datos_procesados = self.procesar_respuesta(response)
                if datos_procesados:
                    datos_extraidos.append(datos_procesados)

        return datos_extraidos


if __name__ == "__main__":
    try:
        extractor = SuperheroExtractor()
        datos = extractor.ejecutar_extraccion()

        # Crear carpeta data si no existe
        os.makedirs("data", exist_ok=True)

        # Guardar JSON
        with open('data/superheroes_raw.json', 'w') as f:
            json.dump(datos, f, indent=2)
        logger.info("📁 Datos guardados en data/superheroes_raw.json")

        # Guardar CSV
        df = pd.DataFrame(datos)
        df.to_csv('data/superheroes.csv', index=False)
        logger.info("📁 Datos guardados en data/superheroes.csv")

        print("\n" + "="*50)
        print("RESUMEN DE EXTRACCIÓN")
        print("="*50)
        print(df.to_string())
        print("="*50)

    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}")