import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from src.utils.conexion import crear_engine

# ── Configuración ────────────────────────────────────────────────────────────
GOLD_DB = "BI_SaludPublicaTarija_Gold"

st.set_page_config(
    page_title="BI Salud Pública Tarija — Demo Day",
    page_icon="⊕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paleta de colores ────────────────────────────────────────────────────────
COLORS = {
    "primary":    "#4F46E5",
    "secondary":  "#7C3AED",
    "accent":     "#0EA5E9",
    "success":    "#10B981",
    "warning":    "#F59E0B",
    "danger":     "#EF4444",
    "bg_dark":    "#F1F5F9",
    "bg_card":    "#FFFFFF",
    "bg_surface": "#E2E8F0",
    "text":       "#1E293B",
    "text_muted": "#64748B",
}

CHART_PALETTE = ["#4F46E5", "#0EA5E9", "#10B981", "#F59E0B", "#EF4444", "#7C3AED", "#EC4899", "#14B8A6"]

# ── SVG Icon helper ──────────────────────────────────────────────────────────
# Inline SVG icons (Lucide style) for a clean professional look without emojis.
# Each icon is a function returning an SVG string with configurable size/color.

def _svg(paths, size=20, color="currentColor", vbox="0 0 24 24"):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" '
        f'viewBox="{vbox}" fill="none" stroke="{color}" stroke-width="2" '
        f'stroke-linecap="round" stroke-linejoin="round" '
        f'style="display:inline-block;vertical-align:middle;flex-shrink:0;">{paths}</svg>'
    )

ICONS = {
    # Sidebar / branding
    "hospital": lambda s=28, c="#818CF8": _svg(
        '<path d="M12 6v4"/><path d="M14 14h-4"/><path d="M14 18h-4"/>'
        '<path d="M14 8h-4"/><path d="M18 12h2a2 2 0 0 1 2 2v6a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2v-9a2 2 0 0 1 2-2h2"/>'
        '<path d="M18 22V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v18"/>', s, c),

    # KPI icons
    "clipboard": lambda s=24, c="#818CF8": _svg(
        '<rect width="8" height="4" x="8" y="2" rx="1" ry="1"/>'
        '<path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/>'
        '<path d="M12 11h4"/><path d="M12 16h4"/><path d="M8 11h.01"/><path d="M8 16h.01"/>', s, c),
    "clock": lambda s=24, c="#22D3EE": _svg(
        '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>', s, c),
    "user_x": lambda s=24, c="#FCD34D": _svg(
        '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/>'
        '<circle cx="9" cy="7" r="4"/><line x1="17" x2="22" y1="8" y2="13"/>'
        '<line x1="22" x2="17" y1="8" y2="13"/>', s, c),
    "trending_up": lambda s=24, c="#34D399": _svg(
        '<polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>'
        '<polyline points="16 7 22 7 22 13"/>', s, c),
    "check_circle": lambda s=24, c="#34D399": _svg(
        '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>'
        '<polyline points="22 4 12 14.01 9 11.01"/>', s, c),
    "activity": lambda s=24, c="#818CF8": _svg(
        '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>', s, c),

    # Section header icons
    "line_chart": lambda s=18, c="#818CF8": _svg(
        '<path d="M3 3v18h18"/><path d="m19 9-5 5-4-4-3 3"/>', s, c),
    "pie_chart": lambda s=18, c="#818CF8": _svg(
        '<path d="M21.21 15.89A10 10 0 1 1 8 2.83"/>'
        '<path d="M22 12A10 10 0 0 0 12 2v10z"/>', s, c),
    "radio": lambda s=18, c="#818CF8": _svg(
        '<path d="M16.24 7.76a6 6 0 0 1 0 8.49m-8.48-.01a6 6 0 0 1 0-8.49m11.31-2.82a10 10 0 0 1 0 14.14m-14.14 0a10 10 0 0 1 0-14.14"/>'
        '<circle cx="12" cy="12" r="2"/>', s, c),
    "ban": lambda s=18, c="#FB7185": _svg(
        '<circle cx="12" cy="12" r="10"/><path d="m4.9 4.9 14.2 14.2"/>', s, c),
    "building": lambda s=18, c="#818CF8": _svg(
        '<rect width="16" height="20" x="4" y="2" rx="2" ry="2"/>'
        '<path d="M9 22v-4h6v4"/><path d="M8 6h.01"/><path d="M16 6h.01"/>'
        '<path d="M12 6h.01"/><path d="M12 10h.01"/><path d="M12 14h.01"/>'
        '<path d="M16 10h.01"/><path d="M16 14h.01"/><path d="M8 10h.01"/><path d="M8 14h.01"/>', s, c),
    "timer": lambda s=18, c="#22D3EE": _svg(
        '<line x1="10" x2="14" y1="2" y2="2"/><line x1="12" x2="15" y1="14" y2="11"/>'
        '<circle cx="12" cy="14" r="8"/>', s, c),
    "globe": lambda s=18, c="#818CF8": _svg(
        '<circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/>'
        '<path d="M2 12h20"/>', s, c),
    "table": lambda s=18, c="#818CF8": _svg(
        '<path d="M12 3v18"/><rect width="18" height="18" x="3" y="3" rx="2"/>'
        '<path d="M3 9h18"/><path d="M3 15h18"/>', s, c),
    "lightbulb": lambda s=24, c="#FCD34D": _svg(
        '<path d="M15 14c.2-1 .7-1.7 1.5-2.5 1-.9 1.5-2.2 1.5-3.5A6 6 0 0 0 6 8c0 1 .2 2.2 1.5 3.5.7.7 1.3 1.5 1.5 2.5"/>'
        '<path d="M9 18h6"/><path d="M10 22h4"/>', s, c),

    # Sidebar filter icons
    "calendar": lambda s=16, c="#94A3B8": _svg(
        '<path d="M8 2v4"/><path d="M16 2v4"/>'
        '<rect width="18" height="18" x="3" y="4" rx="2"/><path d="M3 10h18"/>', s, c),
    "building_sm": lambda s=16, c="#94A3B8": _svg(
        '<rect width="16" height="20" x="4" y="2" rx="2" ry="2"/>'
        '<path d="M9 22v-4h6v4"/><path d="M8 6h.01"/><path d="M16 6h.01"/>'
        '<path d="M12 6h.01"/><path d="M12 10h.01"/><path d="M12 14h.01"/>'
        '<path d="M16 10h.01"/><path d="M16 14h.01"/><path d="M8 10h.01"/><path d="M8 14h.01"/>', s, c),
    "stethoscope": lambda s=16, c="#94A3B8": _svg(
        '<path d="M4.8 2.3A.3.3 0 1 0 5 2H4a2 2 0 0 0-2 2v5a6 6 0 0 0 6 6v0a6 6 0 0 0 6-6V4a2 2 0 0 0-2-2h-1a.2.2 0 1 0 .3.3"/>'
        '<path d="M8 15v1a6 6 0 0 0 6 6v0a6 6 0 0 0 6-6v-4"/><circle cx="20" cy="10" r="2"/>', s, c),
    "settings": lambda s=18, c="#94A3B8": _svg(
        '<path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/>'
        '<circle cx="12" cy="12" r="3"/>', s, c),

    # Header
    "bar_chart": lambda s=16, c="#818CF8": _svg(
        '<line x1="12" x2="12" y1="20" y2="10"/><line x1="18" x2="18" y1="20" y2="4"/>'
        '<line x1="6" x2="6" y1="20" y2="16"/>', s, c),

    # Footer
    "heart": lambda s=14, c="#818CF8": _svg(
        '<path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/>', s, c),
    "layers": lambda s=14, c="#94A3B8": _svg(
        '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/>'
        '<path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65"/>'
        '<path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65"/>', s, c),
}


# ── Estilos CSS globales ─────────────────────────────────────────────────────
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    .stApp {{
        background: {COLORS["bg_dark"]};
        font-family: 'Inter', sans-serif;
        color: {COLORS["text"]};
    }}

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {{
        background: #FFFFFF !important;
        border-right: 1px solid #E2E8F0;
        box-shadow: 2px 0 8px rgba(0,0,0,0.04);
    }}
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown h1,
    section[data-testid="stSidebar"] .stMarkdown h2,
    section[data-testid="stSidebar"] .stMarkdown h3 {{
        color: {COLORS["text"]} !important;
    }}
    section[data-testid="stSidebar"] label {{
        color: {COLORS["text_muted"]} !important;
        font-weight: 500 !important;
        font-size: 0.85rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    /* ── Clean cards ── */
    .glass-card {{
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }}
    .glass-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(79,70,229,0.10);
    }}

    /* ── KPI Cards ── */
    .kpi-container {{
        display: flex;
        gap: 1.25rem;
        flex-wrap: wrap;
        margin-bottom: 1rem;
    }}
    .kpi-card {{
        flex: 1;
        min-width: 200px;
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 16px;
        padding: 1.25rem 1.5rem;
        position: relative;
        overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }}
    .kpi-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.08);
    }}
    .kpi-card::before {{
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        border-radius: 16px 16px 0 0;
    }}
    .kpi-card.indigo::before {{ background: linear-gradient(90deg, #4F46E5, #818CF8); }}
    .kpi-card.cyan::before {{ background: linear-gradient(90deg, #0EA5E9, #38BDF8); }}
    .kpi-card.emerald::before {{ background: linear-gradient(90deg, #10B981, #34D399); }}
    .kpi-card.amber::before {{ background: linear-gradient(90deg, #F59E0B, #FCD34D); }}
    .kpi-card.rose::before {{ background: linear-gradient(90deg, #F43F5E, #FB7185); }}

    .kpi-icon {{
        margin-bottom: 0.6rem;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 42px;
        height: 42px;
        border-radius: 12px;
        background: #F1F5F9;
        border: 1px solid #E2E8F0;
    }}
    .kpi-card.indigo .kpi-icon {{ background: rgba(79,70,229,0.08); border-color: rgba(79,70,229,0.15); }}
    .kpi-card.cyan .kpi-icon {{ background: rgba(14,165,233,0.08); border-color: rgba(14,165,233,0.15); }}
    .kpi-card.emerald .kpi-icon {{ background: rgba(16,185,129,0.08); border-color: rgba(16,185,129,0.15); }}
    .kpi-card.amber .kpi-icon {{ background: rgba(245,158,11,0.08); border-color: rgba(245,158,11,0.15); }}
    .kpi-card.rose .kpi-icon {{ background: rgba(244,63,94,0.08); border-color: rgba(244,63,94,0.15); }}

    .kpi-label {{
        color: {COLORS["text_muted"]};
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.35rem;
    }}
    .kpi-value {{
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.1;
        margin-bottom: 0.25rem;
    }}
    .kpi-value.indigo {{ color: #4F46E5; }}
    .kpi-value.cyan {{ color: #0EA5E9; }}
    .kpi-value.emerald {{ color: #10B981; }}
    .kpi-value.amber {{ color: #D97706; }}
    .kpi-value.rose {{ color: #E11D48; }}

    .kpi-sub {{
        color: {COLORS["text_muted"]};
        font-size: 0.75rem;
        font-weight: 400;
    }}

    /* ── Header ── */
    .dashboard-header {{
        text-align: center;
        margin-bottom: 2rem;
        padding: 2rem 1rem 1.5rem;
    }}
    .dashboard-header h1 {{
        font-size: 2.25rem;
        font-weight: 800;
        background: linear-gradient(135deg, #4F46E5 0%, #0EA5E9 50%, #10B981 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
        letter-spacing: -0.02em;
    }}
    .dashboard-header p {{
        color: {COLORS["text_muted"]};
        font-size: 1rem;
        font-weight: 400;
        max-width: 700px;
        margin: 0 auto;
        line-height: 1.5;
    }}
    .dashboard-header .badge {{
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: rgba(79, 70, 229, 0.08);
        color: #4F46E5;
        font-size: 0.72rem;
        font-weight: 600;
        padding: 0.3rem 0.85rem;
        border-radius: 999px;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 0.75rem;
        border: 1px solid rgba(79, 70, 229, 0.15);
    }}

    /* ── Section headers ── */
    .section-header {{
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin: 1.5rem 0 1rem 0;
    }}
    .section-header .sh-icon {{
        display: flex;
        align-items: center;
        justify-content: center;
        width: 32px;
        height: 32px;
        border-radius: 8px;
        background: rgba(79,70,229,0.08);
        border: 1px solid rgba(79,70,229,0.12);
        flex-shrink: 0;
    }}
    .section-header h3 {{
        font-size: 1.1rem;
        font-weight: 700;
        color: {COLORS["text"]};
        margin: 0;
        white-space: nowrap;
    }}
    .section-header .line {{
        flex: 1;
        height: 1px;
        background: linear-gradient(90deg, #E2E8F0, transparent);
    }}

    /* ── Tabs styling ── */
    .stTabs [data-baseweb="tab-list"] {{
        background: #FFFFFF;
        border-radius: 12px;
        padding: 0.25rem;
        gap: 0.25rem;
        border: 1px solid #E2E8F0;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 10px;
        color: {COLORS["text_muted"]};
        font-weight: 600;
        font-size: 0.85rem;
        padding: 0.5rem 1.25rem;
    }}
    .stTabs [aria-selected="true"] {{
        background: rgba(79, 70, 229, 0.1) !important;
        color: #4F46E5 !important;
        border: none !important;
    }}
    .stTabs [data-baseweb="tab-highlight"] {{ display: none; }}
    .stTabs [data-baseweb="tab-border"] {{ display: none; }}

    /* ── Plotly chart containers ── */
    .stPlotlyChart {{
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 16px;
        padding: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }}

    .stDataFrame {{ border-radius: 12px; overflow: hidden; }}

    hr {{
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, #E2E8F0, transparent);
        margin: 1.5rem 0;
    }}

    .stMultiSelect > div {{ border-radius: 10px !important; }}

    /* ── Sidebar branding ── */
    .sidebar-brand {{
        text-align: center;
        padding: 1.25rem 0 1.5rem 0;
        border-bottom: 1px solid #E2E8F0;
        margin-bottom: 1.5rem;
    }}
    .sidebar-brand .logo-wrap {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 56px;
        height: 56px;
        border-radius: 16px;
        background: linear-gradient(135deg, rgba(79,70,229,0.1), rgba(14,165,233,0.08));
        border: 1px solid rgba(79,70,229,0.15);
        margin-bottom: 0.6rem;
    }}
    .sidebar-brand .name {{
        font-size: 1.05rem;
        font-weight: 700;
        color: {COLORS["text"]};
    }}
    .sidebar-brand .sub {{
        font-size: 0.7rem;
        color: {COLORS["text_muted"]};
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }}

    /* ── Sidebar filter group ── */
    .filter-title {{
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-size: 0.92rem;
        font-weight: 700;
        color: {COLORS["text"]} !important;
        margin-bottom: 0.75rem;
    }}

    /* ── Footer ── */
    .footer {{
        text-align: center;
        color: {COLORS["text_muted"]};
        font-size: 0.75rem;
        padding: 2rem 0 1rem;
        border-top: 1px solid #E2E8F0;
        margin-top: 2rem;
    }}
    .footer a {{ color: #4F46E5; text-decoration: none; }}
    .footer .footer-icon {{ display: inline-flex; vertical-align: middle; margin: 0 0.15rem; }}

    /* ── Insight card icon ── */
    .insight-icon {{
        display: flex;
        align-items: center;
        justify-content: center;
        width: 44px;
        height: 44px;
        border-radius: 12px;
        background: rgba(245,158,11,0.08);
        border: 1px solid rgba(245,158,11,0.12);
        flex-shrink: 0;
    }}
</style>
""", unsafe_allow_html=True)


# ── Plotly theme helper ──────────────────────────────────────────────────────
def apply_chart_theme(fig, height=420):
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=COLORS["text_muted"], size=12),
        title_font=dict(size=15, color=COLORS["text"], family="Inter, sans-serif"),
        title_x=0.02,
        height=height,
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11, color=COLORS["text_muted"]),
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        xaxis=dict(gridcolor="#F1F5F9", zerolinecolor="#E2E8F0"),
        yaxis=dict(gridcolor="#F1F5F9", zerolinecolor="#E2E8F0"),
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            bordercolor="#E2E8F0",
            font_size=12, font_family="Inter, sans-serif",
            font_color="#1E293B",
        ),
    )
    return fig


# ── Carga de datos ───────────────────────────────────────────────────────────
@st.cache_data
def cargar_datos():
    engine = crear_engine(GOLD_DB)

    df_atenciones = pd.read_sql("""
        SELECT
            DT.fecha, DT.anio, DT.mes, DT.nombre_mes,
            DC.departamento, DC.ciudad, DC.centroSalud,
            DEsp.especialidad, DCan.canalReserva, DEst.estado,
            FA.tiempoEsperaMin, FA.esAtendida, FA.esAusente,
            FA.esCancelada, FA.esReservada, FA.conteoFicha,
            FA.edad_aprox, DC.capacidadDiaria, DCP.valor_cepal
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

    df_atenciones["fecha"] = pd.to_datetime(df_atenciones["fecha"], errors="coerce")
    df_saturacion["fecha"] = pd.to_datetime(df_saturacion["fecha"], errors="coerce")

    return df_atenciones, df_kpis, df_saturacion, df_ausentismo, df_mensual


df_atenciones, df_kpis, df_saturacion, df_ausentismo, df_mensual = cargar_datos()

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div class="sidebar-brand">
        <div class="logo-wrap">{ICONS["hospital"]()}</div>
        <div class="name">Salud Pública BI</div>
        <div class="sub">Tarija · Bolivia</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="filter-title">{ICONS["settings"]()} Filtros de Análisis</div>
    """, unsafe_allow_html=True)

    anios = sorted(df_atenciones["anio"].dropna().unique().tolist())
    centros = sorted(df_atenciones["centroSalud"].dropna().unique().tolist())
    especialidades = sorted(df_atenciones["especialidad"].dropna().unique().tolist())

    anios_sel = st.multiselect("Años", anios, default=anios)
    centros_sel = st.multiselect("Centros de Salud", centros, default=centros)
    especialidades_sel = st.multiselect("Especialidades", especialidades, default=especialidades)

    st.markdown("---")
    st.markdown(f"""
    <div style="text-align:center; padding: 0.5rem 0;">
        <div style="color:{COLORS['text_muted']}; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.25rem;">
            Squad Piola · UPDS
        </div>
        <div style="color:{COLORS['text_muted']}; font-size:0.65rem;">
            INV-0170 — Ing. Nelson Huanca V.
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── Filtrado ─────────────────────────────────────────────────────────────────
df_filtrado = df_atenciones[
    (df_atenciones["anio"].isin(anios_sel)) &
    (df_atenciones["centroSalud"].isin(centros_sel)) &
    (df_atenciones["especialidad"].isin(especialidades_sel))
].copy()

if df_filtrado.empty:
    st.warning("No hay datos para los filtros seleccionados. Ajuste los filtros del panel lateral.")
    st.stop()

# ── Cálculos KPI ─────────────────────────────────────────────────────────────
total_fichas = int(df_filtrado["conteoFicha"].sum())
total_atendidas = int(df_filtrado["esAtendida"].sum())
total_ausentes = int(df_filtrado["esAusente"].sum())
total_canceladas = int(df_filtrado["esCancelada"].sum())
total_cerradas = int(df_filtrado[df_filtrado["estado"].isin(["ATENDIDA", "AUSENTE", "CANCELADA"])]["conteoFicha"].sum())

tasa_ausentismo = round((total_ausentes / total_cerradas) * 100, 2) if total_cerradas > 0 else 0
espera_promedio = round(df_filtrado.loc[df_filtrado["esAtendida"] == 1, "tiempoEsperaMin"].mean(), 1)
tasa_atencion = round((total_atendidas / total_fichas) * 100, 1) if total_fichas > 0 else 0

sat_filtrada = df_saturacion[df_saturacion["centroSalud"].isin(centros_sel)].copy()
sat_filtrada = sat_filtrada[sat_filtrada["anio"].isin(anios_sel)]
indice_saturacion = round(sat_filtrada["indice_saturacion_pct"].mean(), 1) if not sat_filtrada.empty else 0

# ── color helpers for conditional KPIs ──
_aus_class = 'emerald' if tasa_ausentismo < 15 else 'amber' if tasa_ausentismo < 25 else 'rose'
_aus_label = 'Bajo' if tasa_ausentismo < 15 else 'Moderado' if tasa_ausentismo < 25 else 'Crítico'
_sat_class = 'emerald' if indice_saturacion < 80 else 'amber' if indice_saturacion < 100 else 'rose'
_sat_label = 'Normal' if indice_saturacion < 80 else 'Elevada' if indice_saturacion < 100 else 'Sobrecarga'

_aus_icon_color = '#34D399' if tasa_ausentismo < 15 else '#FCD34D' if tasa_ausentismo < 25 else '#FB7185'
_sat_icon_color = '#34D399' if indice_saturacion < 80 else '#FCD34D' if indice_saturacion < 100 else '#FB7185'

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="dashboard-header">
    <div class="badge">{ICONS["bar_chart"]()} Demo Day — Inteligencia de Negocios</div>
    <h1>Resiliencia Operativa en Salud Pública</h1>
    <p>Monitoreo de ausentismo, tiempos de espera, saturación de centros de salud e indicadores CEPALSTAT para la toma de decisiones estratégicas en Tarija.</p>
</div>
""", unsafe_allow_html=True)

# ── KPI Cards ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="kpi-container">
    <div class="kpi-card indigo">
        <div class="kpi-icon">{ICONS["clipboard"]()}</div>
        <div class="kpi-label">Total Fichas Médicas</div>
        <div class="kpi-value indigo">{total_fichas:,}</div>
        <div class="kpi-sub">Registros en el período seleccionado</div>
    </div>
    <div class="kpi-card cyan">
        <div class="kpi-icon">{ICONS["clock"]()}</div>
        <div class="kpi-label">Espera Promedio</div>
        <div class="kpi-value cyan">{espera_promedio} min</div>
        <div class="kpi-sub">Tiempo promedio de pacientes atendidos</div>
    </div>
    <div class="kpi-card {_aus_class}">
        <div class="kpi-icon">{ICONS["user_x"](c=_aus_icon_color)}</div>
        <div class="kpi-label">Tasa de Ausentismo</div>
        <div class="kpi-value {_aus_class}">{tasa_ausentismo}%</div>
        <div class="kpi-sub">{_aus_label} — sobre fichas cerradas</div>
    </div>
    <div class="kpi-card {_sat_class}">
        <div class="kpi-icon">{ICONS["trending_up"](c=_sat_icon_color)}</div>
        <div class="kpi-label">Saturación Promedio</div>
        <div class="kpi-value {_sat_class}">{indice_saturacion}%</div>
        <div class="kpi-sub">{_sat_label} — capacidad diaria</div>
    </div>
    <div class="kpi-card emerald">
        <div class="kpi-icon">{ICONS["check_circle"]()}</div>
        <div class="kpi-label">Tasa de Atención</div>
        <div class="kpi-value emerald">{tasa_atencion}%</div>
        <div class="kpi-sub">{total_atendidas:,} pacientes atendidos</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Tabs principales ─────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "Evolución Temporal",
    "Centros y Especialidades",
    "Contexto CEPAL",
    "Datos Detallados",
])


# ── TAB 1: Evolución temporal ────────────────────────────────────────────────
with tab1:
    st.markdown(f"""
    <div class="section-header">
        <div class="sh-icon">{ICONS["line_chart"]()}</div>
        <h3>Evolución Mensual de Fichas Médicas</h3>
        <div class="line"></div>
    </div>
    """, unsafe_allow_html=True)

    serie = df_filtrado.groupby(["anio", "mes"], as_index=False).agg(
        total_fichas=("conteoFicha", "sum"),
        total_atendidas=("esAtendida", "sum"),
        total_ausentes=("esAusente", "sum"),
        total_canceladas=("esCancelada", "sum"),
    )
    serie["periodo"] = serie["anio"].astype(str) + "-" + serie["mes"].astype(str).str.zfill(2)
    serie = serie.sort_values("periodo")

    fig_serie = go.Figure()
    fig_serie.add_trace(go.Scatter(
        x=serie["periodo"], y=serie["total_fichas"],
        name="Total Fichas", mode="lines+markers",
        line=dict(color=CHART_PALETTE[0], width=2.5), marker=dict(size=6),
        fill="tozeroy", fillcolor="rgba(99,102,241,0.08)",
    ))
    fig_serie.add_trace(go.Scatter(
        x=serie["periodo"], y=serie["total_atendidas"],
        name="Atendidas", mode="lines+markers",
        line=dict(color=CHART_PALETTE[2], width=2), marker=dict(size=5),
    ))
    fig_serie.add_trace(go.Scatter(
        x=serie["periodo"], y=serie["total_ausentes"],
        name="Ausentes", mode="lines+markers",
        line=dict(color=CHART_PALETTE[3], width=2, dash="dot"), marker=dict(size=5),
    ))
    fig_serie.add_trace(go.Scatter(
        x=serie["periodo"], y=serie["total_canceladas"],
        name="Canceladas", mode="lines+markers",
        line=dict(color=CHART_PALETTE[4], width=2, dash="dash"), marker=dict(size=5),
    ))
    apply_chart_theme(fig_serie, height=440)
    fig_serie.update_layout(title="Tendencia mensual: fichas, atenciones, ausencias y cancelaciones")
    st.plotly_chart(fig_serie, width="stretch")

    col_dist1, col_dist2 = st.columns(2)

    with col_dist1:
        st.markdown(f"""
        <div class="section-header">
            <div class="sh-icon">{ICONS["pie_chart"]()}</div>
            <h3>Distribución por Estado</h3>
            <div class="line"></div>
        </div>
        """, unsafe_allow_html=True)

        estados_count = df_filtrado.groupby("estado", as_index=False).agg(
            total=("conteoFicha", "sum")
        ).sort_values("total", ascending=False)

        fig_donut = px.pie(
            estados_count, values="total", names="estado",
            hole=0.55, color_discrete_sequence=CHART_PALETTE,
        )
        apply_chart_theme(fig_donut, height=380)
        fig_donut.update_traces(textposition="inside", textinfo="percent+label", textfont_size=12)
        fig_donut.update_layout(title="Proporción de estados de fichas médicas", showlegend=False)
        st.plotly_chart(fig_donut, width="stretch")

    with col_dist2:
        st.markdown(f"""
        <div class="section-header">
            <div class="sh-icon">{ICONS["radio"]()}</div>
            <h3>Distribución por Canal de Reserva</h3>
            <div class="line"></div>
        </div>
        """, unsafe_allow_html=True)

        canal_count = df_filtrado.groupby("canalReserva", as_index=False).agg(
            total=("conteoFicha", "sum")
        ).sort_values("total", ascending=False)

        fig_canal = px.bar(
            canal_count, x="canalReserva", y="total",
            color="canalReserva", color_discrete_sequence=CHART_PALETTE,
        )
        apply_chart_theme(fig_canal, height=380)
        fig_canal.update_layout(
            title="Fichas por canal de reserva", showlegend=False,
            xaxis_title="Canal", yaxis_title="Total Fichas",
        )
        fig_canal.update_traces(marker_line_width=0, marker_cornerradius=6)
        st.plotly_chart(fig_canal, width="stretch")


# ── TAB 2: Centros y Especialidades ──────────────────────────────────────────
with tab2:
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown(f"""
        <div class="section-header">
            <div class="sh-icon">{ICONS["ban"]()}</div>
            <h3>Ausentismo por Especialidad</h3>
            <div class="line"></div>
        </div>
        """, unsafe_allow_html=True)

        aus_esp = df_filtrado.groupby("especialidad", as_index=False).agg(
            total_fichas=("conteoFicha", "sum"),
            total_ausentes=("esAusente", "sum")
        )
        aus_esp["tasa_ausentismo_pct"] = (aus_esp["total_ausentes"] / aus_esp["total_fichas"] * 100).round(2)
        aus_esp = aus_esp.sort_values("tasa_ausentismo_pct", ascending=True)

        fig_aus = px.bar(
            aus_esp, x="tasa_ausentismo_pct", y="especialidad",
            orientation="h", color="tasa_ausentismo_pct",
            color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"],
        )
        apply_chart_theme(fig_aus, height=500)
        fig_aus.update_layout(
            title="Tasa de ausentismo (%) por especialidad",
            coloraxis_showscale=False, xaxis_title="Tasa de Ausentismo (%)", yaxis_title="",
        )
        fig_aus.update_traces(marker_cornerradius=4)
        st.plotly_chart(fig_aus, width="stretch")

        # ── Drill-down: Centros por Especialidad ──
        especialidades_disponibles = sorted(aus_esp["especialidad"].unique().tolist())
        if especialidades_disponibles:
            esp_sel = st.selectbox(
                "🔍 Seleccione una especialidad para ver el desglose por centro:",
                options=especialidades_disponibles,
                key="drilldown_esp_ausentismo",
            )

            df_drill_esp = df_filtrado[df_filtrado["especialidad"] == esp_sel].copy()

            if not df_drill_esp.empty:
                drill_centro = df_drill_esp.groupby("centroSalud", as_index=False).agg(
                    total_fichas=("conteoFicha", "sum"),
                    atendidas=("esAtendida", "sum"),
                    ausentes=("esAusente", "sum"),
                    canceladas=("esCancelada", "sum"),
                    espera_prom=("tiempoEsperaMin", "mean"),
                )
                drill_centro["tasa_ausentismo"] = (drill_centro["ausentes"] / drill_centro["total_fichas"] * 100).round(1)
                drill_centro["tasa_atencion"] = (drill_centro["atendidas"] / drill_centro["total_fichas"] * 100).round(1)
                drill_centro["espera_prom"] = drill_centro["espera_prom"].round(1)
                drill_centro = drill_centro.sort_values("tasa_ausentismo", ascending=True)

                fig_drill_esp = px.bar(
                    drill_centro, x="tasa_ausentismo", y="centroSalud",
                    orientation="h", color="tasa_ausentismo",
                    color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"],
                    hover_data={
                        "total_fichas": True,
                        "atendidas": True,
                        "ausentes": True,
                        "tasa_atencion": ":.1f",
                        "tasa_ausentismo": ":.1f",
                        "espera_prom": ":.1f",
                    },
                    labels={
                        "centroSalud": "Centro de Salud",
                        "tasa_ausentismo": "Ausentismo %",
                        "total_fichas": "Total Fichas",
                        "atendidas": "Atendidas",
                        "ausentes": "Ausentes",
                        "tasa_atencion": "Tasa Atención %",
                        "espera_prom": "Espera Prom (min)",
                    },
                )
                apply_chart_theme(fig_drill_esp, height=max(250, len(drill_centro) * 45))
                fig_drill_esp.update_layout(
                    title=f"Ausentismo de {esp_sel} por centro",
                    coloraxis_showscale=False,
                    xaxis_title="Tasa de Ausentismo (%)", yaxis_title="",
                    margin=dict(l=40, r=20, t=50, b=30),
                )
                fig_drill_esp.update_traces(marker_cornerradius=4)
                st.plotly_chart(fig_drill_esp, width="stretch")

                with st.expander("📋 Ver tabla detallada por centro"):
                    st.dataframe(
                        drill_centro[["centroSalud", "total_fichas", "atendidas", "ausentes",
                                      "canceladas", "tasa_atencion", "tasa_ausentismo", "espera_prom"]]
                        .sort_values("tasa_ausentismo", ascending=False)
                        .rename(columns={
                            "centroSalud": "Centro de Salud",
                            "total_fichas": "Fichas",
                            "atendidas": "Atendidas",
                            "ausentes": "Ausentes",
                            "canceladas": "Canceladas",
                            "tasa_atencion": "Atención %",
                            "tasa_ausentismo": "Ausentismo %",
                            "espera_prom": "Espera (min)",
                        }),
                        width="stretch",
                        hide_index=True,
                    )
            else:
                st.info("No hay datos para esta especialidad con los filtros actuales.")

    with col_b:
        st.markdown(f"""
        <div class="section-header">
            <div class="sh-icon">{ICONS["building"]()}</div>
            <h3>Saturación Promedio por Centro</h3>
            <div class="line"></div>
        </div>
        """, unsafe_allow_html=True)

        sat_centro = sat_filtrada.groupby("centroSalud", as_index=False).agg(
            indice_saturacion_pct=("indice_saturacion_pct", "mean")
        ).sort_values("indice_saturacion_pct", ascending=True)
        sat_centro["indice_saturacion_pct"] = sat_centro["indice_saturacion_pct"].round(1)

        fig_sat = px.bar(
            sat_centro, x="indice_saturacion_pct", y="centroSalud",
            orientation="h", color="indice_saturacion_pct",
            color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"],
        )
        apply_chart_theme(fig_sat, height=500)
        fig_sat.update_layout(
            title="Índice de saturación promedio (%) por centro",
            coloraxis_showscale=False, xaxis_title="Saturación (%)", yaxis_title="",
        )
        fig_sat.update_traces(marker_cornerradius=4)
        st.plotly_chart(fig_sat, width="stretch")

        # ── Drill-down: Especialidades por Centro ──
        centros_disponibles = sorted(sat_centro["centroSalud"].unique().tolist())
        if centros_disponibles:
            centro_sel = st.selectbox(
                "🔍 Seleccione un centro para ver el desglose por especialidad:",
                options=centros_disponibles,
                key="drilldown_centro_saturacion",
            )

            df_drill = df_filtrado[df_filtrado["centroSalud"] == centro_sel].copy()

            if not df_drill.empty:
                drill_esp = df_drill.groupby("especialidad", as_index=False).agg(
                    total_fichas=("conteoFicha", "sum"),
                    atendidas=("esAtendida", "sum"),
                    ausentes=("esAusente", "sum"),
                    canceladas=("esCancelada", "sum"),
                    espera_prom=("tiempoEsperaMin", "mean"),
                )
                drill_esp["tasa_atencion"] = (drill_esp["atendidas"] / drill_esp["total_fichas"] * 100).round(1)
                drill_esp["tasa_ausentismo"] = (drill_esp["ausentes"] / drill_esp["total_fichas"] * 100).round(1)
                drill_esp["espera_prom"] = drill_esp["espera_prom"].round(1)
                drill_esp = drill_esp.sort_values("total_fichas", ascending=False)

                # Mini bar chart of specialties
                fig_drill = px.bar(
                    drill_esp, x="total_fichas", y="especialidad",
                    orientation="h", color="tasa_ausentismo",
                    color_continuous_scale=["#10B981", "#F59E0B", "#EF4444"],
                    hover_data={
                        "total_fichas": True,
                        "atendidas": True,
                        "ausentes": True,
                        "tasa_atencion": ":.1f",
                        "tasa_ausentismo": ":.1f",
                        "espera_prom": ":.1f",
                    },
                    labels={
                        "total_fichas": "Total Fichas",
                        "especialidad": "Especialidad",
                        "tasa_ausentismo": "Ausentismo %",
                        "atendidas": "Atendidas",
                        "ausentes": "Ausentes",
                        "tasa_atencion": "Tasa Atención %",
                        "espera_prom": "Espera Prom (min)",
                    },
                )
                apply_chart_theme(fig_drill, height=max(250, len(drill_esp) * 38))
                fig_drill.update_layout(
                    title=f"Especialidades en {centro_sel}",
                    coloraxis_showscale=False,
                    xaxis_title="Total Fichas", yaxis_title="",
                    margin=dict(l=40, r=20, t=50, b=30),
                )
                fig_drill.update_traces(marker_cornerradius=4)
                st.plotly_chart(fig_drill, width="stretch")

                # Summary table inside an expander
                with st.expander("📋 Ver tabla detallada de especialidades"):
                    st.dataframe(
                        drill_esp[["especialidad", "total_fichas", "atendidas", "ausentes",
                                   "canceladas", "tasa_atencion", "tasa_ausentismo", "espera_prom"]]
                        .rename(columns={
                            "especialidad": "Especialidad",
                            "total_fichas": "Fichas",
                            "atendidas": "Atendidas",
                            "ausentes": "Ausentes",
                            "canceladas": "Canceladas",
                            "tasa_atencion": "Atención %",
                            "tasa_ausentismo": "Ausentismo %",
                            "espera_prom": "Espera (min)",
                        }),
                        width="stretch",
                        hide_index=True,
                    )
            else:
                st.info("No hay datos de atenciones para este centro con los filtros actuales.")

    st.markdown(f"""
    <div class="section-header">
        <div class="sh-icon">{ICONS["timer"]()}</div>
        <h3>Tiempo de Espera Promedio por Especialidad</h3>
        <div class="line"></div>
    </div>
    """, unsafe_allow_html=True)

    espera_esp = df_filtrado[df_filtrado["esAtendida"] == 1].groupby("especialidad", as_index=False).agg(
        espera_promedio=("tiempoEsperaMin", "mean")
    )
    espera_esp["espera_promedio"] = espera_esp["espera_promedio"].round(1)
    espera_esp = espera_esp.sort_values("espera_promedio", ascending=False)

    fig_espera = px.bar(
        espera_esp, x="especialidad", y="espera_promedio",
        color="espera_promedio", color_continuous_scale=["#06B6D4", "#6366F1", "#EF4444"],
    )
    apply_chart_theme(fig_espera, height=400)
    fig_espera.update_layout(
        title="Minutos promedio de espera por especialidad (solo fichas atendidas)",
        coloraxis_showscale=False, xaxis_title="", yaxis_title="Minutos",
    )
    fig_espera.update_traces(marker_cornerradius=6)
    st.plotly_chart(fig_espera, width="stretch")


# ── TAB 3: Contexto CEPAL ───────────────────────────────────────────────────
with tab3:
    st.markdown(f"""
    <div class="section-header">
        <div class="sh-icon">{ICONS["globe"]()}</div>
        <h3>Demanda Operativa vs. Contexto Externo (CEPALSTAT)</h3>
        <div class="line"></div>
    </div>
    """, unsafe_allow_html=True)

    ce_anual = df_filtrado.groupby("anio", as_index=False).agg(
        total_fichas=("conteoFicha", "sum"),
        valor_cepal=("valor_cepal", "max"),
    )

    fig_cepal = go.Figure()
    fig_cepal.add_trace(go.Bar(
        x=ce_anual["anio"], y=ce_anual["total_fichas"],
        name="Total Fichas", marker_color=CHART_PALETTE[0],
        marker_cornerradius=6, opacity=0.75, yaxis="y",
    ))
    fig_cepal.add_trace(go.Scatter(
        x=ce_anual["anio"], y=ce_anual["valor_cepal"],
        name="Indicador CEPAL", mode="lines+markers",
        line=dict(color=CHART_PALETTE[3], width=3),
        marker=dict(size=9, symbol="diamond", color=CHART_PALETTE[3]),
        yaxis="y2",
    ))
    apply_chart_theme(fig_cepal, height=460)
    fig_cepal.update_layout(
        title="Comparativa anual: volumen de fichas médicas vs. indicador CEPALSTAT",
        yaxis=dict(title="Total Fichas", side="left", gridcolor="#F1F5F9"),
        yaxis2=dict(title="Valor CEPAL", overlaying="y", side="right", gridcolor="#F1F5F9"),
        barmode="overlay",
    )
    st.plotly_chart(fig_cepal, width="stretch")

    st.markdown(f"""
    <div class="glass-card" style="margin-top: 1rem;">
        <div style="display: flex; align-items: flex-start; gap: 1rem;">
            <div class="insight-icon">{ICONS["lightbulb"]()}</div>
            <div>
                <div style="font-weight: 700; font-size: 1rem; margin-bottom: 0.35rem; color: {COLORS['text']};">
                    ¿Qué es el indicador CEPAL?
                </div>
                <div style="color: {COLORS['text_muted']}; font-size: 0.88rem; line-height: 1.6;">
                    El indicador de CEPALSTAT mide variables socioeconómicas relevantes que pueden incidir
                    en la demanda de servicios de salud. Al superponer este dato con el volumen operativo,
                    se busca identificar correlaciones entre el contexto externo y la presión sobre los
                    centros de salud de Tarija — una perspectiva clave para la planificación estratégica
                    y la resiliencia operativa del sistema público.
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── TAB 4: Datos detallados ──────────────────────────────────────────────────
with tab4:
    st.markdown(f"""
    <div class="section-header">
        <div class="sh-icon">{ICONS["table"]()}</div>
        <h3>Registros Filtrados</h3>
        <div class="line"></div>
    </div>
    """, unsafe_allow_html=True)

    col_info1, col_info2, col_info3, col_info4 = st.columns(4)
    col_info1.metric("Total registros", f"{len(df_filtrado):,}")
    col_info2.metric("Centros activos", df_filtrado["centroSalud"].nunique())
    col_info3.metric("Especialidades", df_filtrado["especialidad"].nunique())
    col_info4.metric("Período", f"{df_filtrado['anio'].min()} – {df_filtrado['anio'].max()}")

    st.dataframe(
        df_filtrado[[
            "fecha", "centroSalud", "especialidad", "estado",
            "canalReserva", "tiempoEsperaMin", "edad_aprox", "departamento", "ciudad"
        ]].sort_values("fecha", ascending=False),
        width="stretch",
        height=500,
    )


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="footer">
    <span class="footer-icon">{ICONS["heart"]()}</span> Proyecto BI — <b>Resiliencia Operativa en Salud Pública</b> · Tarija, Bolivia<br>
    Squad Piola · UPDS · INV-0170 · Docente: Ing. Nelson Huanca Victoria<br>
    <span class="footer-icon">{ICONS["layers"]()}</span>
    <span style="color: {COLORS['text_muted']};">Arquitectura Medallón: Bronze → Silver → Gold → Visualización</span>
</div>
""", unsafe_allow_html=True)