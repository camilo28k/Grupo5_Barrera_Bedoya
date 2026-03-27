#!/usr/bin/env python3
"""
consultas.py — Análisis de datos almacenados en PostgreSQL/Supabase.
"""
import sys
sys.path.insert(0, '.')

import pandas as pd
from sqlalchemy import func
from scripts.database import SessionLocal
from scripts.models import Superheroe, MetricasETL
import scripts.models  # noqa: F401

db = SessionLocal()


def promedio_atributos():
    registros = db.query(
        func.avg(Superheroe.inteligencia).label('inteligencia'),
        func.avg(Superheroe.fuerza).label('fuerza'),
        func.avg(Superheroe.velocidad).label('velocidad'),
        func.avg(Superheroe.poder).label('poder'),
    ).all()

    df = pd.DataFrame(registros, columns=['Inteligencia', 'Fuerza', 'Velocidad', 'Poder'])
    df = df.round(1)

    print("\n📊 PROMEDIO DE ATRIBUTOS:")
    print(df.to_string(index=False))


def top_superheroes_promedio():
    registros = db.query(
        Superheroe.nombre,
        Superheroe.editor,
        Superheroe.promedio_poder,
        Superheroe.ranking
    ).order_by(
        Superheroe.promedio_poder.desc()
    ).limit(10).all()

    df = pd.DataFrame(registros, columns=['Nombre', 'Editorial', 'Promedio Poder', 'Ranking'])
    print("\n🏆 TOP 10 SUPERHÉROES:")
    print(df.to_string(index=False))


def editorial_mas_fuerte():
    registros = db.query(
        Superheroe.editor,
        func.avg(Superheroe.promedio_poder).label('promedio_editor'),
    ).group_by(Superheroe.editor).order_by(
        func.avg(Superheroe.promedio_poder).desc()
    ).all()

    if registros:
        top = registros[0]
        print(f"\n🏢 EDITORIAL MÁS FUERTE: {top.editor} con {top.promedio_editor:.1f} promedio")


def heroe_mas_poderoso():
    registro = db.query(
        Superheroe.nombre,
        Superheroe.poder,
        Superheroe.fecha_extraccion,
    ).order_by(
        Superheroe.poder.desc()
    ).first()

    if registro:
        print(
            f"\n⚡ HÉROE MÁS PODEROSO: {registro.nombre} "
            f"con {registro.poder} puntos "
            f"({registro.fecha_extraccion})"
        )


def metricas_etl():
    metricas = db.query(MetricasETL).order_by(
        MetricasETL.fecha_ejecucion.desc()
    ).limit(5).all()

    print("\n📈 ÚLTIMAS 5 EJECUCIONES DEL ETL:")
    if metricas:
        for m in metricas:
            print(
                f"  [{m.estado}] {m.fecha_ejecucion} — "
                f"{m.registros_guardados}/{m.registros_extraidos} registros "
                f"en {m.tiempo_ejecucion_segundos:.2f}s"
            )
    else:
        print("  (Sin ejecuciones registradas aún)")


if __name__ == "__main__":
    try:
        print("\n" + "=" * 50)
        print("ANÁLISIS DE DATOS — POSTGRESQL/SUPABASE")
        print("=" * 50)

        promedio_atributos()
        top_superheroes_promedio()
        editorial_mas_fuerte()
        heroe_mas_poderoso()
        metricas_etl()

        print("\n" + "=" * 50 + "\n")
    finally:
        db.close()