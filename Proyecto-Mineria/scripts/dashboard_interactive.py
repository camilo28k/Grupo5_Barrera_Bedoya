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


st.set_page_config(
    page_title="Dashboard Interactivo - Superheroes",
    page_icon="🦸",
    layout="wide",
)

APP_VERSION = "FINAL-DEBUG-TOPATTR-103"

st.title("🦸 Dashboard Interactivo - Superheroes")
st.caption(f"Version: {APP_VERSION}")
st.markdown("---")

st.error("VERSION UNICA ACTIVA")
st.write("Archivo ejecutado:", __file__)
st.write("Hora ejecucion:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
st.write("Plotly:", plotly.__version__)


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

    numeric_cols = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["nombre", "editor", "alineacion"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

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
        title=f"Top {top_n} por {attr}",
        text=attr,
    )

    fig.update_traces(
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
        margin=dict(t=60, b=80, l=40, r=20),
    )

    max_val = float(df_plot[attr].max()) if not df_plot.empty else 0
    fig.update_yaxes(range=[0, max_val * 1.15 if max_val > 0 else 1])

    return fig


st.sidebar.title("🔧 Filtros")
if st.sidebar.button("Recargar datos"):
    st.rerun()

try:
    df = cargar_datos()
except Exception as e:
    st.error(f"Error cargando datos: {e}")
    st.stop()

st.success(f"Registros cargados: {len(df)}")

with st.expander("Diagnostico general", expanded=False):
    st.write(df.head(10))
    st.write(df[["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]].describe())

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

st.subheader("📊 KPIs")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Heroes", int(len(df_filtrado)))
c2.metric("Promedio Inteligencia", round(df_filtrado["inteligencia"].mean(), 2))
c3.metric("Promedio Fuerza", round(df_filtrado["fuerza"].mean(), 2))
c4.metric("Editoriales", int(df_filtrado["editor"].nunique()))

st.markdown("---")
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

    with st.expander(f"DEBUG REAL {attr}", expanded=(attr in ["inteligencia", "fuerza"])):
        st.write(top_attr[["nombre", attr, "editor", "alineacion"]])
        st.write("Valores unicos:", sorted(top_attr[attr].dropna().unique().tolist()))
        st.write("Max:", top_attr[attr].max(), "Min:", top_attr[attr].min())

    fig = make_bar_chart(top_attr[["nombre", attr]], attr, top_n)

    if i % 2 == 0:
        col1.plotly_chart(fig, use_container_width=True)
    else:
        col2.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.subheader("📈 Inteligencia vs Fuerza")

scatter_df = df_filtrado[["nombre", "editor", "alineacion", "inteligencia", "fuerza"]].dropna().copy()

with st.expander("Debug scatter", expanded=False):
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