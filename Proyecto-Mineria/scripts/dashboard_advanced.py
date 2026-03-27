#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path

# ==============================
# Configuración
# ==============================

st.set_page_config(
    page_title="Dashboard Avanzado Superhéroes",
    page_icon="🦸",
    layout="wide"
)

st.title("🦸 Dashboard Avanzado - Superhéroes")
st.markdown("---")

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ==============================
# Conexión a PostgreSQL
# ==============================

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ==============================
# Cargar datos
# ==============================

@st.cache_data
def cargar_datos():
    query = "SELECT * FROM superheroes"
    df = pd.read_sql(query, engine)
    return df

df = cargar_datos()

# ==============================
# Tabs principales
# ==============================

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Vista General",
    "🏆 Rankings",
    "📈 Análisis",
    "📋 Datos"
])

# ==============================
# TAB 1
# ==============================

with tab1:

    st.subheader("Métricas Generales")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Héroes", len(df))

    with col2:
        st.metric("Promedio Inteligencia", round(df["inteligencia"].mean(),2))

    with col3:
        st.metric("Promedio Fuerza", round(df["fuerza"].mean(),2))

    with col4:
        st.metric("Editoriales", df["editor"].nunique())

    st.markdown("---")

    st.subheader("Top 10 Superhéroes")

    top10 = df.sort_values("promedio_poder", ascending=False).head(10)

    fig = px.bar(
        top10,
        x="nombre",
        y="promedio_poder",
        color="promedio_poder",
        title="Top 10 por Promedio de Poder"
    )

    st.plotly_chart(fig, use_container_width=True)

# ==============================
# TAB 2
# ==============================

with tab2:

    st.subheader("Rankings por Habilidad")

    col1, col2 = st.columns(2)

    with col1:

        top_int = df.sort_values("inteligencia", ascending=False).head(10)

        fig = px.bar(
            top_int,
            x="nombre",
            y="inteligencia",
            title="Top 10 Inteligencia"
        )

        st.plotly_chart(fig, use_container_width=True)

        top_vel = df.sort_values("velocidad", ascending=False).head(10)

        fig = px.bar(
            top_vel,
            x="nombre",
            y="velocidad",
            title="Top 10 Velocidad"
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:

        top_fuerza = df.sort_values("fuerza", ascending=False).head(10)

        fig = px.bar(
            top_fuerza,
            x="nombre",
            y="fuerza",
            title="Top 10 Fuerza"
        )

        st.plotly_chart(fig, use_container_width=True)

        top_poder = df.sort_values("poder", ascending=False).head(10)

        fig = px.bar(
            top_poder,
            x="nombre",
            y="poder",
            title="Top 10 Poder"
        )

        st.plotly_chart(fig, use_container_width=True)

# ==============================
# TAB 3
# ==============================

with tab3:

    st.subheader("Análisis de Datos")

    col1, col2 = st.columns(2)

    with col1:

        fig = px.scatter(
            df,
            x="inteligencia",
            y="fuerza",
            color="editor",
            hover_data=["nombre"],
            title="Inteligencia vs Fuerza"
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:

        editor_count = df["editor"].value_counts().reset_index()
        editor_count.columns = ["editor", "count"]

        fig = px.pie(
            editor_count,
            names="editor",
            values="count",
            title="Distribución por Editorial"
        )

        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    alineacion_count = df["alineacion"].value_counts().reset_index()
    alineacion_count.columns = ["alineacion","count"]

    fig = px.bar(
        alineacion_count,
        x="alineacion",
        y="count",
        title="Distribución por Alineación"
    )

    st.plotly_chart(fig, use_container_width=True)

# ==============================
# TAB 4
# ==============================

with tab4:

    st.subheader("Datos Completos")

    st.dataframe(
        df.sort_values("ranking"),
        use_container_width=True,
        height=500
    )   