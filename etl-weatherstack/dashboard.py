import streamlit as st
import psycopg2
import pandas as pd


st.set_page_config(page_title="Weather ETL Dashboard", layout="wide")

st.title("🌦️ Dashboard Clima - WeatherStack ETL")

def cargar_datos():
    try:
        conn = psycopg2.connect(
            dbname="weather_etl",
            user="etl_user",
            password="1234",
            host="localhost"
        )

        query = "SELECT * FROM clima ORDER BY fecha_extraccion DESC;"
        df = pd.read_sql(query, conn)

        conn.close()
        return df

    except Exception as e:
        st.error(f"Error conectando a la base de datos: {e}")
        return None


df = cargar_datos()

if df is not None and not df.empty:


    col1, col2, col3 = st.columns(3)

    col1.metric("🌍 Ciudades únicas", df["ciudad"].nunique())
    col2.metric("🌡️ Temperatura promedio", round(df["temperatura"].mean(), 2))
    col3.metric("💧 Humedad promedio", round(df["humedad"].mean(), 2))

    st.divider()

    ciudades = df["ciudad"].unique()
    ciudad_seleccionada = st.selectbox("Selecciona una ciudad", ciudades)

    df_filtrado = df[df["ciudad"] == ciudad_seleccionada]

    st.subheader(f"📈 Evolución del clima en {ciudad_seleccionada}")

    st.line_chart(
        df_filtrado.set_index("fecha_extraccion")[["temperatura", "humedad"]]
    )

    st.subheader("📋 Últimos registros")

    st.dataframe(df_filtrado.tail(10))

else:
    st.warning("No hay datos disponibles en la base de datos.")
