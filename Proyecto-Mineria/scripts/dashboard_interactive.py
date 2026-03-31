#!/usr/bin/env python3
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ==============================
# Configuración de la página
# ==============================
st.set_page_config(
    page_title="Dashboard Interactivo - Superhéroes",
    page_icon="🦸",
    layout="wide",
)

st.title("🦸 Dashboard Interactivo - Superhéroes")
st.markdown("---")


# ==============================
# Rutas
# ==============================
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "superheroes.csv"


# ==============================
# Utilidades
# ==============================
def get_secret_or_env(key: str, default: str | None = None) -> str | None:
    """Busca primero en st.secrets y luego en variables de entorno."""
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.getenv(key, default)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas y tipos."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # Renombres por si vienen nombres levemente distintos
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
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    required_columns = [
        "nombre",
        "inteligencia",
        "fuerza",
        "velocidad",
        "durabilidad",
        "poder",
        "combate",
        "editor",
        "alineacion",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    numeric_cols = [
        "inteligencia",
        "fuerza",
        "velocidad",
        "durabilidad",
        "poder",
        "combate",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["nombre", "editor", "alineacion"]:
        df[col] = df[col].astype(str).str.strip()

    df["editor"] = df["editor"].replace({"": "Desconocido", "nan": "Desconocido"})
    df["alineacion"] = df["alineacion"].replace({"": "Desconocido", "nan": "Desconocido"})
    df["nombre"] = df["nombre"].replace({"": "Sin nombre", "nan": "Sin nombre"})

    df = df.dropna(subset=numeric_cols, how="all")
    df = df.sort_values("nombre").reset_index(drop=True)

    return df


def get_db_engine():
    """Crea engine para PostgreSQL usando secrets/env."""
    db_host = get_secret_or_env("DB_HOST")
    db_port = get_secret_or_env("DB_PORT", "5432")
    db_user = get_secret_or_env("DB_USER")
    db_password = get_secret_or_env("DB_PASSWORD")
    db_name = get_secret_or_env("DB_NAME")

    if not all([db_host, db_port, db_user, db_password, db_name]):
        return None

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
        pool_pre_ping=True,
    )


@st.cache_data(ttl=600)
def cargar_datos_desde_db() -> pd.DataFrame:
    """Carga datos desde PostgreSQL."""
    engine = get_db_engine()
    if engine is None:
        raise ValueError("No hay credenciales de base de datos configuradas.")

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
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return clean_dataframe(df)


@st.cache_data
def cargar_datos_desde_csv() -> pd.DataFrame:
    """Carga datos desde CSV local."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo CSV en: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    return clean_dataframe(df)


def cargar_datos() -> tuple[pd.DataFrame, str]:
    """Intenta DB primero, luego CSV."""
    try:
        df = cargar_datos_desde_db()
        return df, "Base de datos"
    except Exception as db_error:
        try:
            df = cargar_datos_desde_csv()
            st.warning(f"No se pudo cargar desde base de datos. Usando CSV local. Detalle: {db_error}")
            return df, "CSV local"
        except Exception as csv_error:
            raise RuntimeError(
                f"No se pudo cargar ni desde base de datos ni desde CSV.\n"
                f"DB: {db_error}\nCSV: {csv_error}"
            )


def make_bar_chart(df_plot: pd.DataFrame, attr: str, top_n: int):
    """Genera gráfico de barras para un atributo."""
    fig = px.bar(
        df_plot,
        x="nombre",
        y=attr,
        color=attr,
        title=f"💪 Top {top_n} Héroes por {attr.capitalize()}",
        color_continuous_scale="Viridis",
        hover_data=["editor", "alineacion"],
    )
    fig.update_layout(
        xaxis_title="Nombre",
        yaxis_title=attr.capitalize(),
        xaxis_tickangle=-30,
        title_x=0,
    )
    return fig


# ==============================
# Cargar datos
# ==============================
try:
    df, fuente_datos = cargar_datos()
except Exception as e:
    st.error(f"No se pudieron cargar los datos: {e}")
    st.stop()

st.caption(f"Fuente de datos: {fuente_datos} | Registros cargados: {len(df)}")


# ==============================
# Sidebar - Filtros
# ==============================
st.sidebar.title("🔧 Filtros")

alineacion_options = ["Todos"] + sorted(df["alineacion"].dropna().astype(str).unique().tolist())
alineacion = st.sidebar.selectbox("Selecciona Alineación:", alineacion_options)

editor_options = ["Todos"] + sorted(df["editor"].dropna().astype(str).unique().tolist())
editor = st.sidebar.selectbox("Selecciona Editorial:", editor_options)

top_n = st.sidebar.slider("Número de Héroes Top", min_value=5, max_value=20, value=10)

df_filtrado = df.copy()

if alineacion != "Todos":
    df_filtrado = df_filtrado[df_filtrado["alineacion"] == alineacion]

if editor != "Todos":
    df_filtrado = df_filtrado[df_filtrado["editor"] == editor]

if df_filtrado.empty:
    st.warning("No hay datos para los filtros seleccionados.")
    st.stop()


# ==============================
# KPIs principales
# ==============================
st.subheader("📊 Indicadores Principales")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Héroes", int(len(df_filtrado)))
with col2:
    st.metric("Promedio Inteligencia", round(df_filtrado["inteligencia"].mean(), 2))
with col3:
    st.metric("Promedio Fuerza", round(df_filtrado["fuerza"].mean(), 2))
with col4:
    st.metric("Editoriales", int(df_filtrado["editor"].nunique()))

st.markdown("---")


# ==============================
# Visualizaciones Top N
# ==============================
st.subheader(f"📊 Top {top_n} Héroes por Atributos")

numeric_cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
col1, col2 = st.columns(2)

for i, attr in enumerate(numeric_cols):
    top_attr = (
        df_filtrado[["nombre", "editor", "alineacion", attr]]
        .dropna(subset=[attr])
        .sort_values(by=attr, ascending=False)
        .head(top_n)
    )

    fig = make_bar_chart(top_attr, attr, top_n)

    with col1 if i % 2 == 0 else col2:
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ==============================
# Comparativa Inteligencia vs Fuerza
# ==============================
st.subheader("📈 Inteligencia vs Fuerza")

fig_scatter = px.scatter(
    df_filtrado,
    x="inteligencia",
    y="fuerza",
    hover_data=["nombre", "editor", "alineacion"],
    color="editor",
    title="📊 Inteligencia vs Fuerza por Editorial",
)

fig_scatter.update_layout(
    xaxis_title="Inteligencia",
    yaxis_title="Fuerza",
    title_x=0,
)

st.plotly_chart(fig_scatter, use_container_width=True)


# ==============================
# Distribución por Alineación
# ==============================
st.subheader("⚖️ Distribución por Alineación")

alineacion_count = df_filtrado["alineacion"].value_counts().reset_index()
alineacion_count.columns = ["alineacion", "count"]

fig_bar = px.bar(
    alineacion_count,
    x="alineacion",
    y="count",
    color="count",
    title="Héroes por Alineación",
)

fig_bar.update_layout(
    xaxis_title="Alineación",
    yaxis_title="Cantidad",
    title_x=0,
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
    default_columns = ["nombre", "inteligencia", "fuerza", "poder", "editor", "alineacion"]
    columnas_mostrar = st.multiselect(
        "Columnas a mostrar:",
        df_filtrado.columns.tolist(),
        default=default_columns,
    )

if not columnas_mostrar:
    st.info("Selecciona al menos una columna para mostrar.")
else:
    df_tabla = df_filtrado[columnas_mostrar]
    if mostrar_todos:
        st.dataframe(df_tabla, use_container_width=True, height=600)
    else:
        st.dataframe(df_tabla.head(top_n), use_container_width=True)


# ==============================
# Descargar CSV filtrado
# ==============================
st.markdown("---")
csv = df_filtrado.to_csv(index=False).encode("utf-8")

st.download_button(
    label="⬇️ Descargar CSV Filtrado",
    data=csv,
    file_name=f"superheroes_filtrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)