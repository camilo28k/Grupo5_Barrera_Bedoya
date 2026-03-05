    #!/usr/bin/env python3
    import json
    import pandas as pd
    import logging
    from pathlib import Path
    import psycopg2
    import os
    from dotenv import load_dotenv

    # ==============================
    # Configuración
    # ==============================

    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"

    load_dotenv(BASE_DIR / ".env")

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


    # ==============================
    # Guardar en base de datos
    # ==============================

    def guardar_en_db(df):

        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        cur = conn.cursor()

        for _, row in df.iterrows():

            cur.execute("""
            INSERT INTO superheroes (
                id,nombre,inteligencia,fuerza,velocidad,
                durabilidad,poder,combate,promedio_poder,ranking
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                int(row["id"]),
                row["nombre"],
                int(row["inteligencia"]),
                int(row["fuerza"]),
                int(row["velocidad"]),
                int(row["durabilidad"]),
                int(row["poder"]),
                int(row["combate"]),
                float(row["promedio_poder"]),
                int(row["ranking"])
            ))

        conn.commit()
        cur.close()
        conn.close()

        logger.info("Datos guardados en PostgreSQL")


    # ==============================
    # Transformación
    # ==============================

    def main():

        ruta_raw = DATA_DIR / "superheroes_raw.json"

        if not ruta_raw.exists():
            logger.error("No existe superheroes_raw.json")
            return

        with open(ruta_raw, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(f"Registros encontrados: {len(data)}")

        heroes = []

        for hero in data:

            powerstats = hero.get("powerstats", {})
            biography = hero.get("biography", {})

            heroes.append({
                "id": hero.get("id"),
                "nombre": hero.get("name"),
                "inteligencia": powerstats.get("intelligence"),
                "fuerza": powerstats.get("strength"),
                "velocidad": powerstats.get("speed"),
                "durabilidad": powerstats.get("durability"),
                "poder": powerstats.get("power"),
                "combate": powerstats.get("combat"),
                "editor": biography.get("publisher"),
                "alineacion": biography.get("alignment")
            })

        df = pd.DataFrame(heroes)

        # ==============================
        # Limpieza de datos
        # ==============================

        numeric_cols = [
            'inteligencia','fuerza','velocidad',
            'durabilidad','poder','combate'
        ]

        # convertir a número (si falla → NaN)
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.drop_duplicates(subset="id")

        # ==============================
        # Métricas
        # ==============================

        df["promedio_poder"] = df[numeric_cols].mean(axis=1)
        df["total_poder"] = df[numeric_cols].sum(axis=1)
        df["ranking"] = df["promedio_poder"].rank(ascending=False, method="min")

        # ==============================
        # LIMPIEZA FINAL (ANTES DE DB)
        # ==============================

        df = df.fillna(0)

        cols_int = [
            "inteligencia",
            "fuerza",
            "velocidad",
            "durabilidad",
            "poder",
            "combate",
            "ranking"
        ]

        for c in cols_int:
            if c in df.columns:
                df[c] = df[c].astype(int)

        df["promedio_poder"] = df["promedio_poder"].astype(float)

        df = df.sort_values(by="promedio_poder", ascending=False)

        top10 = df.head(10)

        # ==============================
        # Guardar archivos
        # ==============================

        df.to_csv(DATA_DIR / "superheroes.csv", index=False)
        top10.to_csv(DATA_DIR / "top10_superheroes.csv", index=False)

        logger.info("CSV generado correctamente")

        # ==============================
        # Guardar en DB
        # ==============================

        guardar_en_db(df)


    # ==============================
    # Main
    # ==============================

    if __name__ == "__main__":
        main()