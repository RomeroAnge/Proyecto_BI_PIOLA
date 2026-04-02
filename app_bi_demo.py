import pandas as pd
import streamlit as st
import plotly.express as px

from src.utils.conexion import crear_engine

GOLD_DB = "BI_SaludPublicaTarija_Gold"

st.set_page_config(
    page_title="Demo Day BI - Salud Pública Tarija",
    layout="wide"
)


@st.cache_data
def cargar_datos():
    engine = crear_engine(GOLD_DB)

    df_atenciones = pd.read_sql("""
        SELECT
            DT.fecha,
            DT.anio,
            DT.mes,
            DT.nombre_mes,
            DC.departamento,
            DC.ciudad,
            DC.centroSalud,
            DEsp.especialidad,
            DCan.canalReserva,
            DEst.estado,
            FA.tiempoEsperaMin,
            FA.esAtendida,
            FA.esAusente,
            FA.esCancelada,
            FA.esReservada,
            FA.conteoFicha,
            FA.edad_aprox,
            DC.capacidadDiaria,
            DCP.valor_cepal
        FROM FactAtenciones FA
        INNER JOIN DimTiempo DT ON FA.fecha_key = DT.fecha_key
        INNER JOIN DimCentroSalud DC ON FA.centro_key = DC.centro_key
        INNER JOIN DimEspecialidad DEsp ON FA.especialidad_key = DEsp.especialidad_key
        INNER JOIN DimCanal DCan ON FA.canal_key = DCan.canal_key
        INNER JOIN DimEstado DEst ON FA.estado_key = DEst.estado_key
        LEFT JOIN DimContextoCEPAL DCP ON FA.contexto_key = DCP.contexto_key
    """, engine)

    df_kpis = pd.read_sql("SELECT * FROM vw_kpis_generales", engine)
    df_saturacion = pd.read_sql("SELECT * FROM vw_saturacion_diaria", engine)
    df_ausentismo = pd.read_sql("SELECT * FROM vw_ausentismo_especialidad", engine)
    df_mensual = pd.read_sql("SELECT * FROM vw_serie_mensual", engine)
    df_campanas = pd.read_sql("SELECT * FROM vw_campanas_resumen", engine)

    df_atenciones["fecha"] = pd.to_datetime(df_atenciones["fecha"], errors="coerce")
    df_saturacion["fecha"] = pd.to_datetime(df_saturacion["fecha"], errors="coerce")

    return df_atenciones, df_kpis, df_saturacion, df_ausentismo, df_mensual, df_campanas


df_atenciones, df_kpis, df_saturacion, df_ausentismo, df_mensual, df_campanas = cargar_datos()

st.title("DEMO DAY BI — Resiliencia Operativa en Salud Pública")
st.markdown(
    "Dashboard analítico para monitorear ausentismo, tiempos de espera, saturación de centros de salud y contexto externo de CEPALSTAT."
)

st.sidebar.header("Filtros")

anios = sorted(df_atenciones["anio"].dropna().unique().tolist())
centros = sorted(df_atenciones["centroSalud"].dropna().unique().tolist())
especialidades = sorted(df_atenciones["especialidad"].dropna().unique().tolist())

anios_sel = st.sidebar.multiselect("Años", anios, default=anios)
centros_sel = st.sidebar.multiselect("Centros de salud", centros, default=centros)
especialidades_sel = st.sidebar.multiselect("Especialidades", especialidades, default=especialidades)

df_filtrado = df_atenciones[
    (df_atenciones["anio"].isin(anios_sel)) &
    (df_atenciones["centroSalud"].isin(centros_sel)) &
    (df_atenciones["especialidad"].isin(especialidades_sel))
].copy()

if df_filtrado.empty:
    st.warning("No hay datos para los filtros seleccionados.")
    st.stop()

# KPIs filtrados
total_fichas = int(df_filtrado["conteoFicha"].sum())
total_ausentes = int(df_filtrado["esAusente"].sum())
total_cerradas = int(df_filtrado[df_filtrado["estado"].isin(["ATENDIDA", "AUSENTE", "CANCELADA"])]["conteoFicha"].sum())

tasa_ausentismo = round((total_ausentes / total_cerradas) * 100, 2) if total_cerradas > 0 else 0
espera_promedio = round(df_filtrado.loc[df_filtrado["esAtendida"] == 1, "tiempoEsperaMin"].mean(), 2)

sat_filtrada = df_saturacion[df_saturacion["centroSalud"].isin(centros_sel)].copy()
sat_filtrada = sat_filtrada[sat_filtrada["anio"].isin(anios_sel)]
indice_saturacion = round(sat_filtrada["indice_saturacion_pct"].mean(), 2) if not sat_filtrada.empty else 0

c1, c2, c3 = st.columns(3)
c1.metric("Tasa de Ausentismo (%)", tasa_ausentismo)
c2.metric("Tiempo Promedio de Espera (min)", espera_promedio)
c3.metric("Índice de Saturación Promedio (%)", indice_saturacion)

st.markdown("---")

# Serie mensual
serie = df_filtrado.groupby(["anio", "mes"], as_index=False).agg(
    total_fichas=("conteoFicha", "sum"),
    total_atendidas=("esAtendida", "sum"),
    total_ausentes=("esAusente", "sum")
)
serie["periodo"] = serie["anio"].astype(str) + "-" + serie["mes"].astype(str).str.zfill(2)

fig_serie = px.line(
    serie,
    x="periodo",
    y=["total_fichas", "total_atendidas", "total_ausentes"],
    markers=True,
    title="Evolución mensual de fichas"
)
st.plotly_chart(fig_serie, width='stretch')

col1, col2 = st.columns(2)

with col1:
    aus_esp = df_filtrado.groupby("especialidad", as_index=False).agg(
        total_fichas=("conteoFicha", "sum"),
        total_ausentes=("esAusente", "sum")
    )
    aus_esp["tasa_ausentismo_pct"] = (aus_esp["total_ausentes"] / aus_esp["total_fichas"] * 100).round(2)
    aus_esp = aus_esp.sort_values("tasa_ausentismo_pct", ascending=False)

    fig_aus = px.bar(
        aus_esp,
        x="especialidad",
        y="tasa_ausentismo_pct",
        title="Ausentismo por especialidad"
    )
    st.plotly_chart(fig_aus, width='stretch')

with col2:
    sat_centro = sat_filtrada.groupby("centroSalud", as_index=False).agg(
        indice_saturacion_pct=("indice_saturacion_pct", "mean")
    ).sort_values("indice_saturacion_pct", ascending=False)

    fig_sat = px.bar(
        sat_centro,
        x="centroSalud",
        y="indice_saturacion_pct",
        title="Saturación promedio por centro"
    )
    st.plotly_chart(fig_sat, width='stretch')

st.markdown("---")

col3, col4 = st.columns(2)

with col3:
    ce_anual = df_filtrado.groupby("anio", as_index=False).agg(
        total_fichas=("conteoFicha", "sum"),
        valor_cepal=("valor_cepal", "max")
    )

    fig_cepal = px.line(
        ce_anual,
        x="anio",
        y=["total_fichas", "valor_cepal"],
        markers=True,
        title="Demanda operativa vs contexto CEPAL"
    )
    st.plotly_chart(fig_cepal, width='stretch')

with col4:
    camp_filtradas = df_campanas[df_campanas["centroSalud"].isin(centros_sel)].copy()
    fig_camp = px.bar(
        camp_filtradas,
        x="tipo",
        y="total_campanas",
        color="estado",
        barmode="group",
        title="Resumen de campañas preventivas"
    )
    st.plotly_chart(fig_camp, width='stretch')

st.markdown("---")
st.subheader("Detalle de registros filtrados")
st.dataframe(df_filtrado, width='stretch', height=350)