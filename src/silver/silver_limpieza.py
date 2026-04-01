import json
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)


def normalizar_texto(valor):
    if pd.isna(valor):
        return valor
    return str(valor).strip().upper()


def convertir_codigo_anio(valor):
    try:
        valor = int(valor)
        if 1900 <= valor <= 2100:
            return valor
        return valor - 27170
    except Exception:
        return None


def limpiar_fichas():
    ruta = BRONZE_DIR / "fichas_bronze.csv"
    df = pd.read_csv(ruta)

    # Conversión de tipos
    df["fechaHoraReserva"] = pd.to_datetime(df["fechaHoraReserva"], errors="coerce")
    df["fechaHoraAtencion"] = pd.to_datetime(df["fechaHoraAtencion"], errors="coerce")
    df["fechaNacimiento"] = pd.to_datetime(df["fechaNacimiento"], errors="coerce")
    df["tiempoEsperaMin"] = pd.to_numeric(df["tiempoEsperaMin"], errors="coerce")

    # Normalización de textos
    columnas_texto = [
        "estado", "prioridad", "canalReserva", "motivoReprogramacion",
        "resultadoAtencion", "indicaciones", "diagnostico",
        "nombrePaciente", "nombreMedico", "especialidad",
        "centroSalud", "departamento", "ciudad", "zona", "area"
    ]

    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_texto)

    # Estandarización de valores inconsistentes
    if "diagnostico" in df.columns:
        df["diagnostico"] = df["diagnostico"].replace({
            "INFECCION RESPIRATORIA AGUDA": "IRA"
        })

    if "resultadoAtencion" in df.columns:
        df["resultadoAtencion"] = df["resultadoAtencion"].replace({
            "ATENCION EXITOSA": "CONSULTA RESUELTA"
        })

    # Tratamiento de nulos descriptivos
    columnas_rellenar = [
        "motivoReprogramacion", "resultadoAtencion",
        "indicaciones", "diagnostico", "zona"
    ]
    for col in columnas_rellenar:
        if col in df.columns:
            df[col] = df[col].fillna("SIN DATO")

    # Derivadas de fecha
    df["anio"] = df["fechaHoraReserva"].dt.year
    df["mes"] = df["fechaHoraReserva"].dt.month
    df["dia"] = df["fechaHoraReserva"].dt.day
    df["fecha"] = df["fechaHoraReserva"].dt.date

    # Edad aproximada
    df["edad_aprox"] = ((pd.Timestamp.today() - df["fechaNacimiento"]).dt.days / 365.25).fillna(0).astype(int)

    # Limpieza final
    df = df.drop_duplicates(subset=["codigoFicha"])
    df = df.dropna(subset=["codigoFicha", "fechaHoraReserva", "estado", "centroSalud"])

    ruta_salida = SILVER_DIR / "fichas_silver.csv"
    df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")
    print(f"Archivo generado: {ruta_salida}")

    return df


def limpiar_campanas():
    ruta = BRONZE_DIR / "campanas_bronze.csv"
    df = pd.read_csv(ruta)

    # Fechas
    df["fechaInicio"] = pd.to_datetime(df["fechaInicio"], errors="coerce")
    df["fechaFin"] = pd.to_datetime(df["fechaFin"], errors="coerce")

    # Normalización de textos
    columnas_texto = [
        "tipo", "titulo", "descripcion", "lugar",
        "requisitos", "publicoObjetivo", "estado", "centroSalud"
    ]
    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_texto)

    # Limpieza específica de URL
    if "imagenUrl" in df.columns:
        df["imagenUrl"] = df["imagenUrl"].astype(str).str.strip()
        df["imagenUrl"] = df["imagenUrl"].replace({"nan": None, "None": None})

    # Relleno de nulos
    for col in ["imagenUrl", "requisitos", "publicoObjetivo"]:
        if col in df.columns:
            df[col] = df[col].fillna("SIN DATO")

    # Derivada
    df["duracionDias"] = (df["fechaFin"] - df["fechaInicio"]).dt.days

    # Limpieza final
    df = df.drop_duplicates(subset=["codigoCampana"])
    df = df.dropna(subset=["codigoCampana", "fechaInicio", "fechaFin", "estado"])

    ruta_salida = SILVER_DIR / "campanas_silver.csv"
    df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")
    print(f"Archivo generado: {ruta_salida}")

    return df


def limpiar_cepal():
    ruta = BRONZE_DIR / "cepal_bronze.json"

    with open(ruta, "r", encoding="utf-8") as archivo:
        data = json.load(archivo)

    if isinstance(data, dict) and "body" in data and "data" in data["body"]:
        registros = data["body"]["data"]
    elif isinstance(data, dict) and "data" in data:
        registros = data["data"]
    elif isinstance(data, list):
        registros = data
    else:
        raise ValueError("No se pudo reconocer la estructura del JSON de CEPAL.")

    df = pd.json_normalize(registros)

    posibles_columnas_anio = ["dim_29117", "dim_1", "Anio", "anio", "year", "Year"]
    col_anio = next((c for c in posibles_columnas_anio if c in df.columns), None)

    if col_anio is None:
        raise ValueError("No se encontró una columna de año en los datos de CEPAL.")

    if "value" not in df.columns:
        raise ValueError("No se encontró la columna 'value' en los datos de CEPAL.")

    df["anio"] = df[col_anio].apply(convertir_codigo_anio)
    df["valor_cepal"] = pd.to_numeric(df["value"], errors="coerce")

    if "iso3" in df.columns:
        df["pais"] = df["iso3"].astype(str).str.upper().str.strip()
    else:
        df["pais"] = "BOL"

    df = df[["anio", "pais", "valor_cepal"]].copy()
    df = df.dropna(subset=["anio", "valor_cepal"])
    df["anio"] = df["anio"].astype(int)

    # Bolivia
    df = df[df["pais"].isin(["BOL", "BOLIVIA"])]

    df = df.drop_duplicates(subset=["anio"])

    ruta_salida = SILVER_DIR / "cepal_silver.csv"
    df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")
    print(f"Archivo generado: {ruta_salida}")

    return df


def generar_resumen_limpieza(df_fichas, df_campanas, df_cepal):
    resumen = {
        "filas_fichas": int(len(df_fichas)),
        "filas_campanas": int(len(df_campanas)),
        "filas_cepal": int(len(df_cepal)),
        "nulos_fichas": df_fichas.isnull().sum().to_dict(),
        "nulos_campanas": df_campanas.isnull().sum().to_dict(),
        "nulos_cepal": df_cepal.isnull().sum().to_dict()
    }

    ruta = SILVER_DIR / "resumen_limpieza.json"
    with open(ruta, "w", encoding="utf-8") as archivo:
        json.dump(resumen, archivo, ensure_ascii=False, indent=2)

    print(f"Archivo generado: {ruta}")


def validar_silver(df_fichas, df_campanas, df_cepal):
    assert df_fichas["codigoFicha"].duplicated().sum() == 0, "Hay fichas duplicadas"
    assert df_campanas["codigoCampana"].duplicated().sum() == 0, "Hay campañas duplicadas"
    assert df_fichas["fechaHoraReserva"].isna().sum() == 0, "Hay fichas sin fechaHoraReserva"
    assert df_fichas["estado"].isna().sum() == 0, "Hay fichas sin estado"
    assert df_fichas["centroSalud"].isna().sum() == 0, "Hay fichas sin centroSalud"
    assert df_cepal["valor_cepal"].isna().sum() == 0, "Hay valores nulos en CEPAL"
    assert (df_campanas["duracionDias"] >= 0).all(), "Hay campañas con duración negativa"
    print("Validaciones Silver superadas correctamente.")


if __name__ == "__main__":
    print("Iniciando proceso Silver...")

    df_fichas = limpiar_fichas()
    df_campanas = limpiar_campanas()
    df_cepal = limpiar_cepal()

    validar_silver(df_fichas, df_campanas, df_cepal)
    generar_resumen_limpieza(df_fichas, df_campanas, df_cepal)

    print("Proceso Silver completado correctamente.")