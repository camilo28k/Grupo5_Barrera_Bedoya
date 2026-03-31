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
# CONFIGURACION
# ==============================
st.set_page_config(
    page_title="Dashboard Interactivo - Superheroes",
    page_icon="🦸",
    layout="wide",
)

APP_VERSION = "FINAL-SUPABASE-NO-CACHE-102"

st.title("🦸 Dashboard Interactivo - Superheroes")
st.caption(f"Version: {APP_VERSION}")
st.markdown("---")


# ==============================
# DEBUG
# ==============================
st.error("VERSION UNICA ACTIVA")
st.write("Archivo ejecutado:", __file__)
st.write("Hora ejecucion:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
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
        raise ValueError("Faltan credenciales de Supabase")

    st.success(f"Conectando a DB: {db_host}:{db_port}")

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
        pool_pre_ping=True,
    )


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
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
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    required_cols = [
        "nombre", "inteligencia", "fuerza", "velocidad",
        "durabilidad", "poder", "combate", "editor", "alineacion"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    numeric_cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
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


def make_bar_chart(df_plot: pd.DataFrame, attr: str, top_n: int):
    fig = px.bar(
        df_plot,
        x="nombre",
        y=attr,
        color=attr,
        color_continuous_scale="Viridis",
        title=f"Top {top_n} por {attr}",
        hover_data={"nombre": False, attr: False},
    )

    fig.update_traces(
        text=df_plot[attr],
        textposition="outside",
        customdata=df_plot[["nombre", attr]].to_numpy(),
        hovertemplate=(
            "nombre=%{customdata[0]}<br>"
            f"{attr}=%{{customdata[1]}}"
            "<extra></extra>"
        ),
        cliponaxis=False,
    )

    fig.update_layout(
        xaxis_title="Nombre",
        yaxis_title=attr.capitalize(),
        xaxis_tickangle=-30,
        title_x=0,
        showlegend=False,
        coloraxis_colorbar_title=attr,
        margin=dict(t=60, b=80, l=40, r=20),
    )

    max_val = float(df_plot[attr].max()) if not df_plot.empty else 0
    fig.update_yaxes(range=[0, max_val * 1.15 if max_val > 0 else 1])

    return fig


# ==============================
# SIDEBAR
# ==============================
st.sidebar.title("🔧 Filtros")

if st.sidebar.button("Recargar datos"):
    st.rerun()


# ==============================
# CARGA DE DATOS
# ==============================
try:
    df = cargar_datos()
except Exception as e:
    st.error(f"Error cargando datos: {e}")
    st.stop()

st.success(f"Registros cargados: {len(df)}")

with st.expander("Diagnostico Datos"):
    st.write(df.head(10))
    st.write(df.describe())
    st.write("Max inteligencia:", df["inteligencia"].max())
    st.write("Min inteligencia:", df["inteligencia"].min())
    st.write("Max fuerza:", df["fuerza"].max())
    st.write("Min fuerza:", df["fuerza"].min())


# ==============================
# FILTROS
# ==============================
alineacion = st.sidebar.selectbox(
    "Alineacion",
    ["Todos"] + sorted(df["alineacion"].dropna().unique().tolist())
)

editor = st.sidebar.selectbox(
    "Editorial",
    ["Todos"] + sorted(df["editor"].dropna().unique().tolist())
)

top_n = st.sidebar.slider("Top N", 5, 20, 10)

df_filtrado = df.copy()

if alineacion != "Todos":
    df_filtrado = df_filtrado[df_filtrado["alineacion"] == alineacion]

if editor != "Todos":
    df_filtrado = df_filtrado[df_filtrado["editor"] == editor]

if df_filtrado.empty:
    st.warning("No hay datos para los filtros seleccionados.")
    st.stop()


# ==============================
# KPIS
# ==============================
st.subheader("📊 KPIs")
c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Heroes", int(len(df_filtrado)))
c2.metric("Promedio Inteligencia", round(df_filtrado["inteligencia"].mean(), 2))
c3.metric("Promedio Fuerza", round(df_filtrado["fuerza"].mean(), 2))
c4.metric("Editoriales", int(df_filtrado["editor"].nunique()))

st.markdown("---")


# ==============================
# GRAFICOS TOP
# ==============================
st.subheader("📊 Top Heroes por Atributo")

cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
col1, col2 = st.columns(2)

for i, attr in enumerate(cols):
    top_attr = (
        df_filtrado[
            ["nombre", "editor", "alineacion", "inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
        ]
        .dropna(subset=[attr])
        .sort_values(
            by=[attr, "combate", "poder", "velocidad", "nombre"],
            ascending=[False, False, False, False, True],
            kind="mergesort",
        )
        .head(top_n)
        .copy()
    )

    with st.expander(f"Debug {attr}"):
        st.write(top_attr[["nombre", attr, "editor", "alineacion"]])

    fig = make_bar_chart(top_attr[["nombre", attr]], attr, top_n)

    if i % 2 == 0:
        col1.plotly_chart(fig, use_container_width=True)
    else:
        col2.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ==============================
# SCATTER
# ==============================
st.subheader("📈 Inteligencia vs Fuerza")

scatter_df = df_filtrado[["nombre", "editor", "alineacion", "inteligencia", "fuerza"]].dropna().copy()

with st.expander("Debug scatter"):
    st.write(scatter_df.head(20))
    st.write("Filas scatter:", len(scatter_df))

fig_scatter = px.scatter(
    scatter_df,
    x="inteligencia",
    y="fuerza",
    color="editor",
    hover_name="nombre",
    hover_data={
        "alineacion": True,
        "inteligencia": True,
        "fuerza": True,
        "editor": True,
    },
    title="Inteligencia vs Fuerza por Editorial",
)

fig_scatter.update_layout(
    xaxis_title="Inteligencia",
    yaxis_title="Fuerza",
    title_x=0,
)

st.plotly_chart(fig_scatter, use_container_width=True)


# ==============================
# DISTRIBUCION POR ALINEACION
# ==============================
st.markdown("---")
st.subheader("⚖️ Distribucion por Alineacion")

alineacion_count = df_filtrado["alineacion"].value_counts().reset_index()
alineacion_count.columns = ["alineacion", "cantidad"]

fig_alineacion = px.bar(
    alineacion_count,
    x="alineacion",
    y="cantidad",
    color="cantidad",
    title="Heroes por Alineacion",
)

fig_alineacion.update_layout(
    xaxis_title="Alineacion",
    yaxis_title="Cantidad",
    title_x=0,
)

st.plotly_chart(fig_alineacion, use_container_width=True)


# ==============================
# TABLA
# ==============================
st.markdown("---")
st.subheader("📋 Datos detallados")

col_a, col_b = st.columns(2)

with col_a:
    mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)

with col_b:
    default_columns = ["nombre", "inteligencia", "fuerza", "poder", "editor", "alineacion"]
    columnas_mostrar = st.multiselect(
        "Columnas a mostrar",
        df_filtrado.columns.tolist(),
        default=default_columns,
    )

if columnas_mostrar:
    df_tabla = df_filtrado[columnas_mostrar]
    if mostrar_todos:
        st.dataframe(df_tabla, use_container_width=True, height=600)
    else:
        st.dataframe(df_tabla.head(top_n), use_container_width=True)
else:
    st.info("Selecciona al menos una columna.")


# ==============================
# DESCARGA
# ==============================
st.markdown("---")
csv_bytes = df_filtrado.to_csv(index=False).encode("utf-8")

st.download_button(
    label="⬇️ Descargar CSV filtrado",
    data=csv_bytes,
    file_name=f"superheroes_filtrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)