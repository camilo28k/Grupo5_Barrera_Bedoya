#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

# ==============================
# Configuración de la página
# ==============================
st.set_page_config(
    page_title="Dashboard Interactivo - Superhéroes",
    page_icon="🦸",
    layout="wide"
)

st.title("🦸 Dashboard Interactivo - Superhéroes")
st.markdown("---")

# ==============================
# Ruta del CSV generado por el transformador
# ==============================
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "superheroes.csv"

# ==============================
# Cargar datos
# ==============================
@st.cache_data
def cargar_datos():
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.lower()
    return df

try:
    df = cargar_datos()
except Exception as e:
    st.error(f"No se pudo cargar el CSV: {e}")
    st.stop()

# ==============================
# Filtros interactivos
# ==============================
st.sidebar.title("🔧 Filtros")

# Filtro por alineación
alineacion_options = ["Todos"] + sorted(df["alineacion"].dropna().astype(str).unique().tolist())
alineacion = st.sidebar.selectbox("Selecciona Alineación:", alineacion_options)
if alineacion != "Todos":
    df = df[df["alineacion"] == alineacion]

# Filtro por editor
editor_options = ["Todos"] + sorted(df["editor"].dropna().astype(str).unique().tolist())
editor = st.sidebar.selectbox("Selecciona Editorial:", editor_options)
if editor != "Todos":
    df = df[df["editor"] == editor]

# Filtro por top N
top_n = st.sidebar.slider("Número de Héroes Top", min_value=5, max_value=20, value=10)

# ==============================
# KPIs principales
# ==============================
st.subheader("📊 Indicadores Principales")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Héroes", len(df))
with col2:
    st.metric("Promedio Inteligencia", round(df["inteligencia"].mean(), 2))
with col3:
    st.metric("Promedio Fuerza", round(df["fuerza"].mean(), 2))
with col4:
    st.metric("Editoriales", df["editor"].nunique())

st.markdown("---")

# ==============================
# Visualizaciones Top N
# ==============================
st.subheader(f"📊 Top {top_n} Héroes por Atributos")

numeric_cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
col1, col2 = st.columns(2)

for attr in numeric_cols:
    top_attr = df.sort_values(by=attr, ascending=False).head(top_n)
    with col1 if numeric_cols.index(attr) % 2 == 0 else col2:
        fig = px.bar(
            top_attr,
            x="nombre",
            y=attr,
            color=attr,
            title=f"💪 Top {top_n} Héroes por {attr.capitalize()}",
            color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ==============================
# Comparativa Inteligencia vs Fuerza
# ==============================
st.subheader("📈 Inteligencia vs Fuerza")
fig_scatter = px.scatter(
    df,
    x="inteligencia",
    y="fuerza",
    hover_data=["nombre", "editor", "alineacion"],
    color="editor",
    title="📊 Inteligencia vs Fuerza por Editorial"
)
st.plotly_chart(fig_scatter, use_container_width=True)

# ==============================
# Distribución por Alineación
# ==============================
st.subheader("⚖️ Distribución por Alineación")
alineacion_count = df["alineacion"].value_counts().reset_index()
alineacion_count.columns = ["alineacion", "count"]
fig_bar = px.bar(
    alineacion_count,
    x="alineacion",
    y="count",
    color="count",
    title="Héroes por Alineación"
)
st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")

# ==============================
# Tabla de datos interactiva
# ==============================
st.subheader("📋 Datos Detallados")
col1, col2 = st.columns(2)
with col1:
    mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)
with col2:
    columnas_mostrar = st.multiselect(
        "Columnas a mostrar:",
        df.columns.tolist(),
        default=["nombre", "inteligencia", "fuerza", "poder", "editor", "alineacion"]
    )

if mostrar_todos:
    st.dataframe(df[columnas_mostrar], use_container_width=True, height=600)
else:
    st.dataframe(df[columnas_mostrar].head(top_n), use_container_width=True)

# ==============================
# Descargar CSV filtrado
# ==============================
st.markdown("---")
csv = df.to_csv(index=False)
st.download_button(
    label="⬇️ Descargar CSV Filtrado",
    data=csv,
    file_name=f"superheroes_filtrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv"
)