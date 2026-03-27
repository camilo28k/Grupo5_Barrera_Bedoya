#!/usr/bin/env python3
"""
extractor_db.py — Fase Load del ETL con persistencia en PostgreSQL/Supabase.
Lee datos de superheroes.csv y los inserta en la base de datos.
"""
import sys
sys.path.insert(0, '.')

import os
import pandas as pd
import time
import logging

from scripts.database import SessionLocal, create_all_tables
from scripts.models import Superheroe, MetricasETL
import scripts.models  # noqa: F401

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SuperheroETLDB:
    def __init__(self):
        create_all_tables()
        self.db = SessionLocal()
        self.tiempo_inicio = time.time()
        self.registros_extraidos = 0
        self.registros_guardados = 0
        self.registros_fallidos = 0

    def _guardar_metricas(self, estado: str):
        try:
            tiempo = round(time.time() - self.tiempo_inicio, 2)
            metricas = MetricasETL(
                registros_extraidos=self.registros_extraidos,
                registros_guardados=self.registros_guardados,
                registros_fallidos=self.registros_fallidos,
                tiempo_ejecucion_segundos=tiempo,
                estado=estado,
                mensaje=(
                    f"{self.registros_guardados} registros guardados de "
                    f"{self.registros_extraidos} en {tiempo}s"
                ),
            )
            self.db.add(metricas)
            self.db.commit()
            logger.info(f"📈 Métricas guardadas — estado: {estado}")
        except Exception as e:
            logger.error(f"❌ Error guardando métricas: {e}")

    def ejecutar(self) -> bool:
        csv_path = 'data/superheroes.csv'
        if not os.path.exists(csv_path):
            logger.error(f"❌ No se encontró {csv_path}. Ejecuta primero transformador.py")
            return False

        logger.info(f"📂 Cargando datos desde {csv_path}")
        df = pd.read_csv(csv_path)
        self.registros_extraidos = len(df)
        logger.info(f"📊 {self.registros_extraidos} registros a procesar")

        ids_existentes = {
            row[0] for row in self.db.query(Superheroe.id).all()
        }

        registros_bulk = []

        for _, fila in df.iterrows():
            try:
                hero_id = int(fila["id"])
                if hero_id in ids_existentes:
                    continue

                registros_bulk.append(
                    Superheroe(
                        id=hero_id,
                        nombre=str(fila.get("nombre", "")),
                        inteligencia=int(fila.get("inteligencia", 0)),
                        fuerza=int(fila.get("fuerza", 0)),
                        velocidad=int(fila.get("velocidad", 0)),
                        durabilidad=int(fila.get("durabilidad", 0)),
                        poder=int(fila.get("poder", 0)),
                        combate=int(fila.get("combate", 0)),
                        nombre_completo=str(fila.get("nombre_completo", "")),
                        editor=str(fila.get("editor", "")),
                        alineacion=str(fila.get("alineacion", "")),
                        genero=str(fila.get("genero", "")),
                        raza=str(fila.get("raza", "")),
                        altura=str(fila.get("altura", "")),
                        peso=str(fila.get("peso", "")),
                        fecha_extraccion=pd.to_datetime(fila.get("fecha_extraccion")),
                        promedio_poder=float(fila.get("promedio_poder", 0)),
                        ranking=float(fila.get("ranking", 0)),
                    )
                )
            except Exception as e:
                logger.warning(f"⚠️ Fila omitida: {e}")
                self.registros_fallidos += 1

        try:
            self.db.bulk_save_objects(registros_bulk)
            self.db.commit()
            self.registros_guardados = len(registros_bulk)
            logger.info(f"✅ Bulk insert completado: {self.registros_guardados} registros")
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Error en bulk insert: {e}")
            return False

        estado = 'SUCCESS' if self.registros_fallidos == 0 else 'PARTIAL'
        self._guardar_metricas(estado)

        logger.info(
            f"✅ ETL completado — Guardados: {self.registros_guardados} | "
            f"Fallidos: {self.registros_fallidos}"
        )
        return True

    def mostrar_resumen(self):
        try:
            total = self.db.query(Superheroe).count()
            print(f"\n📊 RESUMEN EN BASE DE DATOS")
            print(f"   Superhéroes registrados : {total}")
        except Exception as e:
            logger.error(f"❌ Error mostrando resumen: {e}")
        finally:
            self.db.close()


if __name__ == "__main__":
    etl = SuperheroETLDB()
    exito = etl.ejecutar()
    etl.mostrar_resumen()
    sys.exit(0 if exito else 1)