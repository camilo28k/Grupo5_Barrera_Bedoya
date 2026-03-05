#!/usr/bin/env python3
import schedule
import time
import logging
from extractor import SuperheroExtractor
import subprocess
from pathlib import Path

# ==============================
# Logging
# ==============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

def ejecutar_etl():
    logger.info("Iniciando proceso ETL...")

    try:
        # ==========================
        # EXTRACCIÓN
        # ==========================
        extractor = SuperheroExtractor()
        datos = extractor.ejecutar_extraccion(731)

        logger.info(f"Extracción completada: {len(datos)} registros")

        # ==========================
        # TRANSFORMACIÓN
        # ==========================
        subprocess.run(["python", "transformador.py"], cwd=BASE_DIR / "scripts")

        logger.info("Transformación completada")

        # ==========================
        # VISUALIZACIÓN
        # ==========================
        subprocess.run(["python", "visualizador.py"], cwd=BASE_DIR / "scripts")

        logger.info("Visualización completada")

        logger.info("ETL finalizado correctamente")

    except Exception as e:
        logger.error(f"Error en ETL: {e}")

# ==============================
# PROGRAMACIÓN AUTOMÁTICA
# ==============================

# Ejecutar cada 1 hora
schedule.every(1).hours.do(ejecutar_etl)

# Si quieres cada día:
# schedule.every().day.at("08:00").do(ejecutar_etl)

logger.info("Scheduler iniciado. Esperando ejecución...")

while True:
    schedule.run_pending()
    time.sleep(60)