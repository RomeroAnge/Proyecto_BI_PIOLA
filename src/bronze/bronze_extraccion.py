import json
from pathlib import Path

import pandas as pd
import requests

from src.utils.conexion import crear_engine

INDICADOR_ID = 2205
PAIS_BOLIVIA = "221"

BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def extraer_fichas():
    engine = crear_engine()

    query = """
    SELECT
        FM.codigoFicha,
        FM.fechaHoraReserva,
        FM.fechaHoraAtencion,
        FM.estado,
        FM.prioridad,
        FM.canalReserva,
        FM.tiempoEsperaMin,
        FM.motivoReprogramacion,
        FM.resultadoAtencion,
        FM.indicaciones,
        FM.diagnostico,
        P.idPaciente,
        U.nombreCompleto AS nombrePaciente,
        P.sexo,
        P.fechaNacimiento,
        M.idMedico,
        UM.nombreCompleto AS nombreMedico,
        E.nombre AS especialidad,
        C.nombre AS centroSalud,
        C.capacidadDiaria,
        G.departamento,
        G.ciudad,
        G.zona,
        G.area
    FROM FichaMedica FM
    INNER JOIN Paciente P ON FM.idPaciente = P.idPaciente
    INNER JOIN Usuario U ON P.idUsuario = U.idUsuario
    INNER JOIN Medico M ON FM.idMedico = M.idMedico
    INNER JOIN Usuario UM ON M.idUsuario = UM.idUsuario
    INNER JOIN Especialidad E ON FM.idEspecialidad = E.idEspecialidad
    INNER JOIN CentroSalud C ON FM.idCentro = C.idCentro
    INNER JOIN Geografia G ON C.idGeografia = G.idGeografia
    """

    df = pd.read_sql(query, engine)
    ruta = BRONZE_DIR / "fichas_bronze.csv"
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"Archivo generado: {ruta}")


def extraer_campanas():
    engine = crear_engine()

    query = """
    SELECT
        CM.codigoCampana,
        CM.tipo,
        CM.titulo,
        CM.descripcion,
        CM.imagenUrl,
        CM.fechaInicio,
        CM.fechaFin,
        CM.lugar,
        CM.requisitos,
        CM.publicoObjetivo,
        CM.estado,
        C.nombre AS centroSalud
    FROM CampanaMedica CM
    INNER JOIN CentroSalud C ON CM.idCentro = C.idCentro
    """

    df = pd.read_sql(query, engine)
    ruta = BRONZE_DIR / "campanas_bronze.csv"
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"Archivo generado: {ruta}")


def extraer_cepal():
    url = f"https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{INDICADOR_ID}/data"
    params = {
        "members": PAIS_BOLIVIA,
        "lang": "es",
        "format": "json"
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        ruta = BRONZE_DIR / "cepal_bronze.json"
        with open(ruta, "w", encoding="utf-8") as archivo:
            json.dump(data, archivo, ensure_ascii=False, indent=2)

        print(f"Archivo generado: {ruta}")

    except requests.exceptions.RequestException as e:
        print("Error al consumir la API de CEPALSTAT:")
        print(e)


if __name__ == "__main__":
    print("Iniciando extracción Bronze...")
    extraer_fichas()
    extraer_campanas()
    extraer_cepal()
    print("Extracción Bronze completada correctamente.")