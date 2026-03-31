#!/usr/bin/env python3
import os
from datetime import datetime

import pandas as pd
import plotly
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ==============================
# CONFIGURACIÓN
# ==============================
st.set_page_config(
    page_title="Dashboard Interactivo - Superhéroes",
    page_icon="🦸",
    layout="wide",
)

APP_VERSION = "FINAL-SUPABASE-DEBUG-100"

st.title("🦸 Dashboard Interactivo - Superhéroes")
st.caption(f"Versión: {APP_VERSION}")
st.markdown("---")


# ==============================
# 🔥 DEBUG CRÍTICO (MUY IMPORTANTE)
# ==============================
st.error("🚨 VERSION UNICA ACTIVA 🚨")
st.write("Archivo ejecutado:", __file__)
st.write("Hora ejecución:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
st.write("Plotly:", plotly.__version__)


# ==============================
# UTILIDADES
# ==============================
def get_secret_or_env(key: str, default=None):
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.getenv(key, default)


def get_db_engine():
    db_host = get_secret_or_env("DB_HOST")
    db_port = get_secret_or_env("DB_PORT", "5432")
    db_user = get_secret_or_env("DB_USER")
    db_password = get_secret_or_env("DB_PASSWORD")
    db_name = get_secret_or_env("DB_NAME")

    if not all([db_host, db_port, db_user, db_password, db_name]):
        raise ValueError("❌ Faltan credenciales de Supabase")

    st.success(f"✅ Conectando a DB: {db_host}:{db_port}")

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
        pool_pre_ping=True,
    )


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()

    rename_map = {
        "intelligence": "inteligencia",
        "strength": "fuerza",
        "speed": "velocidad",
        "durability": "durabilidad",
        "power": "poder",
        "combat": "combate",
        "alignment": "alineacion",
        "publisher": "editor",
        "name": "nombre",
    }

    df = df.rename(columns=rename_map)

    numeric_cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=numeric_cols, how="all")
    df = df.sort_values("nombre").reset_index(drop=True)

    return df


@st.cache_data(ttl=600)
def cargar_datos():
    engine = get_db_engine()

    query = text("""
        SELECT
            nombre,
            inteligencia,
            fuerza,
            velocidad,
            durabilidad,
            poder,
            combate,
            editor,
            alineacion
        FROM superheroes
        ORDER BY nombre
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return clean_dataframe(df)


# ==============================
# CARGA DE DATOS
# ==============================
try:
    df = cargar_datos()
except Exception as e:
    st.error(f"❌ Error cargando datos: {e}")
    st.stop()

st.success(f"📊 Registros cargados: {len(df)}")

with st.expander("🔍 Diagnóstico Datos"):
    st.write(df.head(10))
    st.write(df.describe())


# ==============================
# FILTROS
# ==============================
st.sidebar.title("🔧 Filtros")

alineacion = st.sidebar.selectbox(
    "Alineación",
    ["Todos"] + sorted(df["alineacion"].dropna().unique())
)

editor = st.sidebar.selectbox(
    "Editorial",
    ["Todos"] + sorted(df["editor"].dropna().unique())
)

top_n = st.sidebar.slider("Top N", 5, 20, 10)

df_filtrado = df.copy()

if alineacion != "Todos":
    df_filtrado = df_filtrado[df_filtrado["alineacion"] == alineacion]

if editor != "Todos":
    df_filtrado = df_filtrado[df_filtrado["editor"] == editor]


# ==============================
# KPIs
# ==============================
st.subheader("📊 KPIs")

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total", len(df_filtrado))
c2.metric("Inteligencia Prom", round(df_filtrado["inteligencia"].mean(), 2))
c3.metric("Fuerza Prom", round(df_filtrado["fuerza"].mean(), 2))
c4.metric("Editoriales", df_filtrado["editor"].nunique())

st.markdown("---")


# ==============================
# GRÁFICOS TOP
# ==============================
st.subheader("📊 Top Héroes")

cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]

col1, col2 = st.columns(2)

for i, attr in enumerate(cols):

    top_attr = (
        df_filtrado[["nombre", attr]]
        .dropna()
        .sort_values(by=[attr, "nombre"], ascending=[False, True])
        .head(top_n)
    )

    with st.expander(f"Debug {attr}"):
        st.write(top_attr)

    fig = px.bar(
        top_attr,
        x="nombre",
        y=attr,
        color=attr,
        color_continuous_scale="Viridis",
        title=f"Top {top_n} por {attr}"
    )

    if i % 2 == 0:
        col1.plotly_chart(fig, use_container_width=True)
    else:
        col2.plotly_chart(fig, use_container_width=True)


# ==============================
# SCATTER
# ==============================
st.subheader("📈 Inteligencia vs Fuerza")

fig = px.scatter(
    df_filtrado,
    x="inteligencia",
    y="fuerza",
    color="editor"
)

st.plotly_chart(fig, use_container_width=True)