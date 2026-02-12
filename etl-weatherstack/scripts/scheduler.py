import schedule
import time

def ejecutar_etl():
    extractor = WeatherstackExtractor()
    extractor.ejecutar_extraccion()

# Ejecutar cada 1 hora
schedule.every(1).hours.do(ejecutar_etl)

while True:
    schedule.run_pending()
    time.sleep(60)
