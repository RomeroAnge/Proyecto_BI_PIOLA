from pathlib import Path
import pandas as pd
from sqlalchemy import text

from src.utils.conexion import crear_engine

BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

GOLD_DB = "BI_SaludPublicaTarija_Gold"


def validar_archivos():
    archivos = [
        BRONZE_DIR / "fichas_bronze.csv",
        BRONZE_DIR / "campanas_bronze.csv",
        BRONZE_DIR / "cepal_bronze.json",
        SILVER_DIR / "fichas_silver.csv",
        SILVER_DIR / "campanas_silver.csv",
        SILVER_DIR / "cepal_silver.csv",
        SILVER_DIR / "resumen_limpieza.json",
        GOLD_DIR / "DimTiempo.csv",
        GOLD_DIR / "DimCentroSalud.csv",
        GOLD_DIR / "DimEspecialidad.csv",
        GOLD_DIR / "DimCanal.csv",
        GOLD_DIR / "DimEstado.csv",
        GOLD_DIR / "DimContextoCEPAL.csv",
        GOLD_DIR / "FactAtenciones.csv",
    ]

    print("=== VALIDACION DE ARCHIVOS ===")
    for archivo in archivos:
        existe = archivo.exists()
        print(f"{archivo.name}: {'OK' if existe else 'FALTA'}")
        assert existe, f"No existe el archivo {archivo}"


def validar_csv():
    print("\n=== VALIDACION DE CSV ===")

    fichas_silver = pd.read_csv(SILVER_DIR / "fichas_silver.csv")
    fact_atenciones = pd.read_csv(GOLD_DIR / "FactAtenciones.csv")
    dim_tiempo = pd.read_csv(GOLD_DIR / "DimTiempo.csv")
    dim_centro = pd.read_csv(GOLD_DIR / "DimCentroSalud.csv")

    print(f"Filas fichas_silver: {len(fichas_silver)}")
    print(f"Filas fact_atenciones: {len(fact_atenciones)}")
    print(f"Filas dim_tiempo: {len(dim_tiempo)}")
    print(f"Filas dim_centro: {len(dim_centro)}")

    assert fichas_silver["codigoFicha"].duplicated().sum() == 0, "Hay codigoFicha duplicado en Silver"
    assert fact_atenciones["atencion_key"].duplicated().sum() == 0, "Hay atencion_key duplicado en Gold"

    claves = ["fecha_key", "centro_key", "especialidad_key", "canal_key", "estado_key"]
    for col in claves:
        nulos = fact_atenciones[col].isna().sum()
        print(f"Nulos en {col}: {nulos}")
        assert nulos == 0, f"Hay nulos en {col} dentro de FactAtenciones"


def validar_sql():
    print("\n=== VALIDACION EN SQL SERVER ===")

    engine = crear_engine(GOLD_DB)

    consultas = {
        "DimTiempo": "SELECT COUNT(*) AS total FROM DimTiempo",
        "DimCentroSalud": "SELECT COUNT(*) AS total FROM DimCentroSalud",
        "DimEspecialidad": "SELECT COUNT(*) AS total FROM DimEspecialidad",
        "DimCanal": "SELECT COUNT(*) AS total FROM DimCanal",
        "DimEstado": "SELECT COUNT(*) AS total FROM DimEstado",
        "DimContextoCEPAL": "SELECT COUNT(*) AS total FROM DimContextoCEPAL",
        "FactAtenciones": "SELECT COUNT(*) AS total FROM FactAtenciones",
        "vw_kpis_generales": "SELECT * FROM vw_kpis_generales",
        "vw_saturacion_diaria": "SELECT TOP 5 * FROM vw_saturacion_diaria",
        "vw_ausentismo_especialidad": "SELECT TOP 5 * FROM vw_ausentismo_especialidad",
        "vw_serie_mensual": "SELECT TOP 5 * FROM vw_serie_mensual",
    }

    for nombre, query in consultas.items():
        df = pd.read_sql(query, engine)
        print(f"\n[{nombre}]")
        print(df.head())


def validar_relaciones():
    print("\n=== VALIDACION DE RELACIONES FISICAS ===")

    engine = crear_engine(GOLD_DB)

    query_fk = """
    SELECT 
        fk.name AS nombre_fk,
        OBJECT_NAME(fk.parent_object_id) AS tabla_hija,
        OBJECT_NAME(fk.referenced_object_id) AS tabla_padre
    FROM sys.foreign_keys fk
    ORDER BY tabla_hija, nombre_fk
    """

    query_pk = """
    SELECT 
        kc.name AS nombre_pk,
        OBJECT_NAME(kc.parent_object_id) AS tabla
    FROM sys.key_constraints kc
    WHERE kc.type = 'PK'
    ORDER BY tabla
    """

    df_fk = pd.read_sql(query_fk, engine)
    df_pk = pd.read_sql(query_pk, engine)

    print("\nPRIMARY KEYS:")
    print(df_pk)

    print("\nFOREIGN KEYS:")
    print(df_fk)

    assert len(df_pk) >= 7, "No se detectaron todas las PK esperadas"
    assert len(df_fk) >= 6, "No se detectaron todas las FK esperadas"


if __name__ == "__main__":
    print("INICIANDO PRUEBAS FINALES DEL PROYECTO...\n")

    validar_archivos()
    validar_csv()
    validar_sql()
    validar_relaciones()

    print("\nTODAS LAS VALIDACIONES FINALES FUERON SUPERADAS CORRECTAMENTE.")