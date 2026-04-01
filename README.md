# PROYECTO FINAL BI - DEMO DAY

## SQUAD PIOLA

**Materia:** Inteligencia de Negocios (INV-0170)  
**Docente:** Ing. Nelson Huanca Victoria  

## Integrantes
- Velasquez Ortega Sergio Noel — Data Engineer
- Romero Vega Cristhian Angelo B. — Scrum Master
- Chorolque Guerrero Juan Manuel — QA

## Descripción del proyecto
Este proyecto desarrolla una solución de Inteligencia de Negocios de punta a punta para analizar la resiliencia operativa del sistema de fichas médicas y campañas preventivas en Tarija, Bolivia.

La propuesta integra datos transaccionales locales almacenados en SQL Server con indicadores externos obtenidos desde CEPALSTAT, aplicando la Arquitectura Medallón: Bronze, Silver, Gold y Visualización.

## Propósito
Transformar datos operativos en información estratégica que ayude a mejorar la toma de decisiones sobre:
- tiempos de espera,
- ausentismo de pacientes,
- saturación de centros de salud,
- planificación de campañas preventivas.

## Alineación con ODS
El proyecto se alinea con el **ODS 9: Industria, innovación e infraestructura**, ya que fortalece la infraestructura digital de apoyo a decisiones en el sector público mediante integración de datos, analítica y visualización.

## Arquitectura del proyecto
- **Bronze:** extracción de datos desde SQL Server y API CEPALSTAT.
- **Silver:** limpieza, normalización y validación de datos con Python y Pandas.
- **Gold:** modelado analítico y cálculo de KPIs/OKRs en SQL Server.
- **Visualización:** dashboard interactivo en Streamlit.

## Estructura del repositorio
```text
sql/
src/
data/
evidencias/