import os
import requests
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class WeatherstackExtractor:

    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.base_url = os.getenv("WEATHERSTACK_BASE_URL")
        self.ciudades = os.getenv("CIUDADES").split(",")

    def obtener_datos_ciudad(self, ciudad):

        params = {
            "access_key": self.api_key,
            "query": ciudad
        }

        try:
            response = requests.get(f"{self.base_url}/current", params=params)
            data = response.json()

            if "current" not in data:
                logger.error(f"Error en API para {ciudad}: {data}")
                return None

            resultado = {
                "ciudad": data["location"]["name"],
                "pais": data["location"]["country"],
                "latitud": data["location"]["lat"],
                "longitud": data["location"]["lon"],
                "temperatura": data["current"]["temperature"],
                "sensacion_termica": data["current"]["feelslike"],
                "humedad": data["current"]["humidity"],
                "velocidad_viento": data["current"]["wind_speed"],
                "descripcion": data["current"]["weather_descriptions"][0],
                "fecha_extraccion": datetime.now(),
                "codigo_tiempo": data["current"]["weather_code"]
            }

            logger.info(f"✅ Datos extraídos para {ciudad}")
            return resultado

        except Exception as e:
            logger.error(f"Error extrayendo datos de {ciudad}: {str(e)}")
            return None

    def ejecutar_extraccion(self):

        logger.info("🚀 Iniciando proceso ETL")

        resultados = []

        for ciudad in self.ciudades:
            dato = self.obtener_datos_ciudad(ciudad.strip())
            if dato:
                resultados.append(dato)

        logger.info(f"Extracción completada. Registros obtenidos: {len(resultados)}")

        return resultados

    def guardar_en_postgres(self, datos):

        try:
            conn = psycopg2.connect(
                dbname="weather_etl",
                user="etl_user",
                password="1234",
                host="localhost"
            )

            cursor = conn.cursor()

            for dato in datos:
                cursor.execute("""
                    INSERT INTO clima (
                        ciudad, pais, latitud, longitud, temperatura,
                        sensacion_termica, humedad, velocidad_viento,
                        descripcion, fecha_extraccion, codigo_tiempo
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    dato['ciudad'],
                    dato['pais'],
                    dato['latitud'],
                    dato['longitud'],
                    dato['temperatura'],
                    dato['sensacion_termica'],
                    dato['humedad'],
                    dato['velocidad_viento'],
                    dato['descripcion'],
                    dato['fecha_extraccion'],
                    dato['codigo_tiempo']
                ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("✅ Datos guardados en PostgreSQL")
            print("✅ Datos guardados en PostgreSQL")

        except Exception as e:
            logger.error(f"Error guardando en PostgreSQL: {str(e)}")
            print(f"❌ Error guardando en PostgreSQL: {str(e)}")



if __name__ == "__main__":

    extractor = WeatherstackExtractor()

    datos = extractor.ejecutar_extraccion()

    if datos:
        extractor.guardar_en_postgres(datos)
        print(f"Registros insertados: {len(datos)}")
    else:
        print("No se obtuvieron datos.")

