#!/usr/bin/env python3
import os
from datetime import datetime

import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
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

APP_VERSION = "FINAL-HABILIDADES-106"

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
        if col in df.columns:
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
        title=f"Top {top_n} por {attr}",
        text=attr,
        color=attr,
        color_continuous_scale="Viridis",
    )

    fig.update_traces(
        textposition="outside",
        hovertemplate=(
            "nombre=%{x}<br>"
            f"{attr}=%{{y}}"
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

with st.expander("Diagnostico general", expanded=False):
    st.write(df.head(10))
    st.write(df[["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]].describe())


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
# TORTA POR HABILIDADES
# ==============================
st.subheader("🥧 Proporcion Promedio por Habilidad")

habilidades = ["inteligencia", "fuerza", "velocidad", "durabilidad", "poder", "combate"]

df_pie = pd.DataFrame({
    "habilidad": habilidades,
    "promedio": [df_filtrado[h].mean() for h in habilidades]
})

fig_pie = px.pie(
    df_pie,
    names="habilidad",
    values="promedio",
    title="Participacion promedio de cada habilidad"
)

st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("---")


# ==============================
# BOXPLOT POR HABILIDADES
# ==============================
st.subheader("📦 Distribucion de Habilidades")

df_box = df_filtrado[habilidades].melt(
    var_name="habilidad",
    value_name="valor"
)

fig_box = px.box(
    df_box,
    x="habilidad",
    y="valor",
    color="habilidad",
    title="Boxplot de habilidades"
)

fig_box.update_layout(
    xaxis_title="Habilidad",
    yaxis_title="Valor",
    title_x=0,
)

st.plotly_chart(fig_box, use_container_width=True)

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

    fig = make_bar_chart(top_attr[["nombre", attr]], attr, top_n)

    if i % 2 == 0:
        col1.plotly_chart(fig, use_container_width=True)
    else:
        col2.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ==============================
# VS ENTRE TOP 10 PODER Y TOP 10 COMBATE
# ==============================
st.markdown("---")
st.subheader("📈 VS entre Top 10 de Poder y Top 10 de Combate")

top_poder = df_filtrado[["nombre", "editor", "alineacion", "poder", "combate"]].copy()
top_poder["poder"] = pd.to_numeric(top_poder["poder"], errors="coerce")
top_poder["combate"] = pd.to_numeric(top_poder["combate"], errors="coerce")
top_poder = (
    top_poder.dropna(subset=["poder", "combate"])
    .sort_values(by=["poder", "combate", "nombre"], ascending=[False, False, True])
    .head(10)
)

top_combate = df_filtrado[["nombre", "editor", "alineacion", "poder", "combate"]].copy()
top_combate["poder"] = pd.to_numeric(top_combate["poder"], errors="coerce")
top_combate["combate"] = pd.to_numeric(top_combate["combate"], errors="coerce")
top_combate = (
    top_combate.dropna(subset=["poder", "combate"])
    .sort_values(by=["combate", "poder", "nombre"], ascending=[False, False, True])
    .head(10)
)

scatter_df = (
    pd.concat([top_poder, top_combate], ignore_index=True)
    .drop_duplicates(subset=["nombre"])
    .reset_index(drop=True)
)

with st.expander("Debug scatter top poder vs combate", expanded=False):
    st.write("Top 10 poder")
    st.write(top_poder)
    st.write("Top 10 combate")
    st.write(top_combate)
    st.write("Puntos finales del VS")
    st.write(scatter_df)
    st.write(
        scatter_df.groupby(["poder", "combate"]).size().reset_index(name="cantidad_en_mismo_punto")
    )

if scatter_df.empty:
    st.warning("No hay datos suficientes para mostrar la gráfica VS.")
else:
    # Jitter pequeño para separar puntos repetidos
    scatter_df = scatter_df.copy()
    scatter_df["idx_en_grupo"] = scatter_df.groupby(["poder", "combate"]).cumcount()

    # desplazamientos pequeños
    offsets = [-2.0, -1.0, 0.0, 1.0, 2.0, -1.5, 1.5, -2.5, 2.5, 3.0]
    scatter_df["poder_plot"] = scatter_df.apply(
        lambda r: r["poder"] + offsets[r["idx_en_grupo"] % len(offsets)],
        axis=1
    )
    scatter_df["combate_plot"] = scatter_df.apply(
        lambda r: r["combate"] + offsets[::-1][r["idx_en_grupo"] % len(offsets)],
        axis=1
    )

    st.scatter_chart(
        scatter_df,
        x="poder_plot",
        y="combate_plot",
        use_container_width=True,
    )

    st.caption(
        "Se aplicó una pequeña separación visual para que héroes con el mismo poder y combate no queden uno encima del otro."
    )

    st.dataframe(
        scatter_df[["nombre", "editor", "alineacion", "poder", "combate"]]
        .sort_values(by=["poder", "combate"], ascending=False),
        use_container_width=True,
    )

# ==============================
# TABLA
# ==============================
st.subheader("📋 Datos Detallados")

col_a, col_b = st.columns(2)

with col_a:
    mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)

with col_b:
    default_columns = ["nombre", "inteligencia", "fuerza", "velocidad", "poder", "editor", "alineacion"]
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