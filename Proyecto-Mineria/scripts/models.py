#!/usr/bin/env python3
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime
from scripts.database import Base


class Superheroe(Base):
    __tablename__ = "superheroes"

    id = Column(Integer, primary_key=True, autoincrement=False)
    nombre = Column(String(100), nullable=True)
    inteligencia = Column(Integer, nullable=True)
    fuerza = Column(Integer, nullable=True)
    velocidad = Column(Integer, nullable=True)
    durabilidad = Column(Integer, nullable=True)
    poder = Column(Integer, nullable=True)
    combate = Column(Integer, nullable=True)
    nombre_completo = Column(String(150), nullable=True)
    editor = Column(String(100), nullable=True)
    alineacion = Column(String(50), nullable=True)
    genero = Column(String(50), nullable=True)
    raza = Column(String(100), nullable=True)
    altura = Column(String(50), nullable=True)
    peso = Column(String(50), nullable=True)
    fecha_extraccion = Column(DateTime, default=datetime.utcnow)
    promedio_poder = Column(Float, nullable=True)
    ranking = Column(Float, nullable=True)

    def __repr__(self):
        return f"<Superheroe(nombre='{self.nombre}', editor='{self.editor}')>"


class MetricasETL(Base):
    __tablename__ = "metricas_etl"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_ejecucion = Column(DateTime, default=datetime.utcnow, index=True)
    registros_extraidos = Column(Integer, nullable=False)
    registros_guardados = Column(Integer, nullable=False)
    registros_fallidos = Column(Integer, default=0)
    tiempo_ejecucion_segundos = Column(Float, nullable=False)
    estado = Column(String(50), nullable=False)
    mensaje = Column(String(500), nullable=True)

    def __repr__(self):
        return f"<MetricasETL(fecha={self.fecha_ejecucion}, estado={self.estado})>"