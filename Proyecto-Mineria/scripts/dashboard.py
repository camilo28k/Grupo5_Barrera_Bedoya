#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ==============================
# Configuración de la página
# ==============================
st.set_page_config(
    page_title="Dashboard Superhéroes",
    page_icon="🦸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================
# Título principal
# ==============================
st.title("🦸 Dashboard de Superhéroes - ETL")
st.markdown("---")

# ==============================
# Ruta del CSV transformado
# ==============================
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "superheroes.csv"

# ==============================
# Cargar datos
# ==============================
@st.cache_data
def cargar_datos_csv():
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.lower()
    return df

try:
    df = cargar_datos_csv()
except Exception as e:
    st.error(f"No se pudo cargar el CSV: {e}")
    st.stop()

# ==============================
# Verificar columnas necesarias
# ==============================
required_cols = [
    "nombre",
    "inteligencia",
    "fuerza",
    "velocidad",
    "durabilidad",
    "poder",
    "combate",
    "editor",
    "alineacion"
]

for col in required_cols:
    if col not in df.columns:
        st.error(f"La columna '{col}' no existe en el CSV.")
        st.stop()

# ==============================
# Limpieza de datos
# ==============================
df = df.fillna(0)

# ==============================
# Sidebar - Filtros
# ==============================
st.sidebar.title("🔧 Filtros")

alineacion_options = ["Todos"] + sorted(df["alineacion"].astype(str).unique().tolist())
alineacion = st.sidebar.selectbox("Selecciona Alineación:", alineacion_options)

if alineacion != "Todos":
    df = df[df["alineacion"] == alineacion]

editor_options = ["Todos"] + sorted(df["editor"].astype(str).unique().tolist())
editor = st.sidebar.selectbox("Selecciona Editorial:", editor_options)

if editor != "Todos":
    df = df[df["editor"] == editor]

# ==============================
# Métricas principales
# ==============================
st.subheader("📈 Métricas Principales")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Héroes", len(df))

with col2:
    st.metric("Promedio Inteligencia", round(df["inteligencia"].mean(), 2))

with col3:
    st.metric("Promedio Fuerza", round(df["fuerza"].mean(), 2))

with col4:
    st.metric("Promedio Poder", round(df["poder"].mean(), 2))

st.markdown("---")

# ==============================
# Columnas numéricas
# ==============================
columnas_numericas = [
    "inteligencia",
    "fuerza",
    "velocidad",
    "durabilidad",
    "poder",
    "combate"
]

# ==============================
# Top 10 por habilidades
# ==============================
st.subheader("🏆 Top 10 por Habilidades")

for col in columnas_numericas:

    top10 = df.sort_values(by=col, ascending=False).head(10)

    fig = px.bar(
        top10,
        x=col,
        y="nombre",
        orientation="h",
        title=f"Top 10 por {col.capitalize()}",
        color=col,
        color_continuous_scale="blues"
    )

    fig.update_layout(yaxis=dict(autorange="reversed"))

    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ==============================
# Gráfica de torta alineación
# ==============================
st.subheader("⚖️ Distribución por Alineación")

alineacion_count = df["alineacion"].value_counts().reset_index()
alineacion_count.columns = ["alineacion", "count"]

fig_pie = px.pie(
    alineacion_count,
    names="alineacion",
    values="count",
    title="Distribución de héroes por alineación"
)

st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("---")

# ==============================
# Tabla de datos
# ==============================
st.subheader("📋 Datos Detallados")

st.dataframe(
    df.sort_values("fuerza", ascending=False),
    use_container_width=True,
    height=400
)