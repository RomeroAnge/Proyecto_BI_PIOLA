from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.utils.conexion import crear_engine

BASE_DIR = Path(__file__).resolve().parents[2]
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

GOLD_DB = "BI_SaludPublicaTarija_Gold"


def crear_base_gold():
    engine_master = crear_engine("master")
    with engine_master.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f"""
        IF DB_ID('{GOLD_DB}') IS NULL
        BEGIN
            CREATE DATABASE [{GOLD_DB}]
        END
        """))
    print(f"Base Gold verificada: {GOLD_DB}")


def cargar_silver():
    fichas = pd.read_csv(SILVER_DIR / "fichas_silver.csv")
    campanas = pd.read_csv(SILVER_DIR / "campanas_silver.csv")
    cepal = pd.read_csv(SILVER_DIR / "cepal_silver.csv")

    fichas["fechaHoraReserva"] = pd.to_datetime(fichas["fechaHoraReserva"], errors="coerce")
    fichas["fechaHoraAtencion"] = pd.to_datetime(fichas["fechaHoraAtencion"], errors="coerce")
    fichas["fechaNacimiento"] = pd.to_datetime(fichas["fechaNacimiento"], errors="coerce")

    campanas["fechaInicio"] = pd.to_datetime(campanas["fechaInicio"], errors="coerce")
    campanas["fechaFin"] = pd.to_datetime(campanas["fechaFin"], errors="coerce")

    cepal["anio"] = pd.to_numeric(cepal["anio"], errors="coerce").astype("Int64")
    cepal["valor_cepal"] = pd.to_numeric(cepal["valor_cepal"], errors="coerce")

    return fichas, campanas, cepal


def normalizar_texto_simple(valor):
    if pd.isna(valor):
        return valor
    return str(valor).strip().upper()


def construir_dim_centro_desde_bronze():
    engine_bronze = crear_engine("BI_SaludPublicaTarija_Bronze")

    query = """
    SELECT
        C.nombre AS centroSalud,
        G.ciudad,
        G.departamento,
        G.area,
        C.capacidadDiaria
    FROM CentroSalud C
    INNER JOIN Geografia G
        ON C.idGeografia = G.idGeografia
    """

    dim = pd.read_sql(query, engine_bronze)

    for col in ["centroSalud", "ciudad", "departamento", "area"]:
        dim[col] = dim[col].apply(normalizar_texto_simple)

    dim = (
        dim.drop_duplicates()
        .sort_values(["departamento", "ciudad", "centroSalud"])
        .reset_index(drop=True)
    )

    dim["centro_key"] = range(1, len(dim) + 1)

    return dim[
        ["centro_key", "centroSalud", "ciudad", "departamento", "area", "capacidadDiaria"]
    ]


def construir_dim_tiempo(fichas, campanas):
    fechas_fichas = pd.to_datetime(fichas["fechaHoraReserva"], errors="coerce").dt.normalize()
    fechas_camp_ini = pd.to_datetime(campanas["fechaInicio"], errors="coerce").dt.normalize()
    fechas_camp_fin = pd.to_datetime(campanas["fechaFin"], errors="coerce").dt.normalize()

    fechas = pd.concat([fechas_fichas, fechas_camp_ini, fechas_camp_fin], ignore_index=True)
    fechas = fechas.dropna().drop_duplicates().sort_values()

    dim = pd.DataFrame({"fecha": fechas})
    dim["fecha_key"] = dim["fecha"].dt.strftime("%Y%m%d").astype(int)
    dim["anio"] = dim["fecha"].dt.year
    dim["trimestre"] = dim["fecha"].dt.quarter
    dim["mes"] = dim["fecha"].dt.month
    dim["dia"] = dim["fecha"].dt.day
    dim["dia_semana"] = dim["fecha"].dt.dayofweek + 1

    meses = {
        1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
        5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
        9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"
    }
    dias = {
        1: "LUNES", 2: "MARTES", 3: "MIERCOLES", 4: "JUEVES",
        5: "VIERNES", 6: "SABADO", 7: "DOMINGO"
    }

    dim["nombre_mes"] = dim["mes"].map(meses)
    dim["nombre_dia"] = dim["dia_semana"].map(dias)

    return dim[
        ["fecha_key", "fecha", "anio", "trimestre", "mes", "nombre_mes", "dia", "dia_semana", "nombre_dia"]
    ]


def construir_dim_especialidad(fichas):
    dim = fichas[["especialidad"]].drop_duplicates().sort_values("especialidad").reset_index(drop=True)
    dim["especialidad_key"] = range(1, len(dim) + 1)
    return dim[["especialidad_key", "especialidad"]]


def construir_dim_canal(fichas):
    dim = fichas[["canalReserva"]].drop_duplicates().sort_values("canalReserva").reset_index(drop=True)
    dim["canal_key"] = range(1, len(dim) + 1)
    return dim[["canal_key", "canalReserva"]]


def construir_dim_estado(fichas):
    dim = fichas[["estado"]].drop_duplicates().sort_values("estado").reset_index(drop=True)
    dim["estado_key"] = range(1, len(dim) + 1)
    return dim[["estado_key", "estado"]]


def construir_dim_cepal(cepal):
    dim = cepal.drop_duplicates(subset=["anio"]).sort_values("anio").reset_index(drop=True)
    dim["contexto_key"] = range(1, len(dim) + 1)
    dim["pais"] = dim["pais"].astype(str).str.upper().str.strip()
    return dim[["contexto_key", "anio", "pais", "valor_cepal"]]


def construir_dim_tipo_campana(campanas):
    dim = campanas[["tipo"]].drop_duplicates().sort_values("tipo").reset_index(drop=True)
    dim["tipo_campana_key"] = range(1, len(dim) + 1)
    return dim[["tipo_campana_key", "tipo"]]


def construir_fact_atenciones(fichas, dim_tiempo, dim_centro, dim_especialidad, dim_canal, dim_estado, dim_cepal):
    fact = fichas.copy()
    fact["fecha"] = pd.to_datetime(fact["fechaHoraReserva"], errors="coerce").dt.normalize()

    fact = fact.merge(dim_tiempo[["fecha_key", "fecha", "anio"]], on=["fecha", "anio"], how="left")
    fact = fact.merge(
        dim_centro,
        on=["centroSalud", "ciudad", "departamento", "area", "capacidadDiaria"],
        how="left"
    )
    fact = fact.merge(dim_especialidad, on="especialidad", how="left")
    fact = fact.merge(dim_canal, on="canalReserva", how="left")
    fact = fact.merge(dim_estado, on="estado", how="left")
    fact = fact.merge(dim_cepal[["contexto_key", "anio"]], on="anio", how="left")

    fact["esAtendida"] = (fact["estado"] == "ATENDIDA").astype(int)
    fact["esAusente"] = (fact["estado"] == "AUSENTE").astype(int)
    fact["esCancelada"] = (fact["estado"] == "CANCELADA").astype(int)
    fact["esReservada"] = (fact["estado"] == "RESERVADA").astype(int)
    fact["conteoFicha"] = 1

    fact = fact.reset_index(drop=True)
    fact["atencion_key"] = range(1, len(fact) + 1)

    return fact[
        [
            "atencion_key", "codigoFicha", "fecha_key", "centro_key", "especialidad_key",
            "canal_key", "estado_key", "contexto_key", "tiempoEsperaMin",
            "edad_aprox", "esAtendida", "esAusente", "esCancelada",
            "esReservada", "conteoFicha"
        ]
    ]


def construir_fact_campanas(campanas, dim_tiempo, dim_centro, dim_tipo):
    fact = campanas.copy()
    fact["fecha_inicio"] = pd.to_datetime(fact["fechaInicio"], errors="coerce").dt.normalize()
    fact["fecha_fin"] = pd.to_datetime(fact["fechaFin"], errors="coerce").dt.normalize()

    fact = fact.merge(
        dim_tiempo[["fecha_key", "fecha"]].rename(
            columns={"fecha_key": "fecha_inicio_key", "fecha": "fecha_inicio"}
        ),
        on="fecha_inicio",
        how="left"
    )

    fact = fact.merge(
        dim_tiempo[["fecha_key", "fecha"]].rename(
            columns={"fecha_key": "fecha_fin_key", "fecha": "fecha_fin"}
        ),
        on="fecha_fin",
        how="left"
    )

    fact = fact.merge(
        dim_centro[["centro_key", "centroSalud"]],
        on="centroSalud",
        how="left"
    )

    fact = fact.merge(
        dim_tipo,
        on="tipo",
        how="left"
    )

    fact["totalCampanas"] = 1
    fact = fact.reset_index(drop=True)
    fact["campana_key"] = range(1, len(fact) + 1)

    return fact[
        [
            "campana_key", "codigoCampana", "centro_key", "tipo_campana_key",
            "fecha_inicio_key", "fecha_fin_key", "estado", "duracionDias", "totalCampanas"
        ]
    ]


def validar_claves_modelo(fact_atenciones, fact_campanas):
    columnas_fact_atenciones = [
        "fecha_key", "centro_key", "especialidad_key",
        "canal_key", "estado_key"
    ]

    columnas_fact_campanas = [
        "centro_key", "tipo_campana_key",
        "fecha_inicio_key", "fecha_fin_key"
    ]

    for col in columnas_fact_atenciones:
        nulos = fact_atenciones[col].isna().sum()
        assert nulos == 0, f"FactAtenciones tiene nulos en {col}: {nulos}"

    for col in columnas_fact_campanas:
        nulos = fact_campanas[col].isna().sum()
        assert nulos == 0, f"FactCampanas tiene nulos en {col}: {nulos}"

    print("Validación de claves del modelo superada correctamente.")


def exportar_csv_locales(tablas):
    for nombre, df in tablas.items():
        ruta = GOLD_DIR / f"{nombre}.csv"
        df.to_csv(ruta, index=False, encoding="utf-8-sig")
        print(f"Archivo generado: {ruta}")


def cargar_tablas_gold_sql(tablas):
    engine_gold = crear_engine(GOLD_DB)

    orden = [
        "DimTiempo",
        "DimCentroSalud",
        "DimEspecialidad",
        "DimCanal",
        "DimEstado",
        "DimContextoCEPAL",
        "DimTipoCampana",
        "FactAtenciones",
        "FactCampanas"
    ]

    for nombre in orden:
        tablas[nombre].to_sql(nombre, engine_gold, if_exists="replace", index=False)
        print(f"Tabla cargada en SQL Server: {nombre}")

    return engine_gold


def crear_relaciones_gold(engine_gold):
    sentencias = [
        # ELIMINAR FOREIGN KEYS SI EXISTEN
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactAtenciones_DimTiempo')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT FK_FactAtenciones_DimTiempo;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactAtenciones_DimCentroSalud')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT FK_FactAtenciones_DimCentroSalud;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactAtenciones_DimEspecialidad')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT FK_FactAtenciones_DimEspecialidad;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactAtenciones_DimCanal')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT FK_FactAtenciones_DimCanal;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactAtenciones_DimEstado')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT FK_FactAtenciones_DimEstado;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactAtenciones_DimContextoCEPAL')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT FK_FactAtenciones_DimContextoCEPAL;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactCampanas_DimCentroSalud')
            ALTER TABLE dbo.FactCampanas DROP CONSTRAINT FK_FactCampanas_DimCentroSalud;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactCampanas_DimTipoCampana')
            ALTER TABLE dbo.FactCampanas DROP CONSTRAINT FK_FactCampanas_DimTipoCampana;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactCampanas_DimTiempoInicio')
            ALTER TABLE dbo.FactCampanas DROP CONSTRAINT FK_FactCampanas_DimTiempoInicio;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_FactCampanas_DimTiempoFin')
            ALTER TABLE dbo.FactCampanas DROP CONSTRAINT FK_FactCampanas_DimTiempoFin;
        """,

        # ELIMINAR PRIMARY KEYS SI EXISTEN
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimTiempo')
            ALTER TABLE dbo.DimTiempo DROP CONSTRAINT PK_DimTiempo;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimCentroSalud')
            ALTER TABLE dbo.DimCentroSalud DROP CONSTRAINT PK_DimCentroSalud;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimEspecialidad')
            ALTER TABLE dbo.DimEspecialidad DROP CONSTRAINT PK_DimEspecialidad;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimCanal')
            ALTER TABLE dbo.DimCanal DROP CONSTRAINT PK_DimCanal;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimEstado')
            ALTER TABLE dbo.DimEstado DROP CONSTRAINT PK_DimEstado;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimContextoCEPAL')
            ALTER TABLE dbo.DimContextoCEPAL DROP CONSTRAINT PK_DimContextoCEPAL;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_DimTipoCampana')
            ALTER TABLE dbo.DimTipoCampana DROP CONSTRAINT PK_DimTipoCampana;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_FactAtenciones')
            ALTER TABLE dbo.FactAtenciones DROP CONSTRAINT PK_FactAtenciones;
        """,
        """
        IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_FactCampanas')
            ALTER TABLE dbo.FactCampanas DROP CONSTRAINT PK_FactCampanas;
        """,

        # ASEGURAR TIPOS DE DATOS Y NULABILIDAD
        "ALTER TABLE dbo.DimTiempo ALTER COLUMN fecha_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.DimCentroSalud ALTER COLUMN centro_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.DimEspecialidad ALTER COLUMN especialidad_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.DimCanal ALTER COLUMN canal_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.DimEstado ALTER COLUMN estado_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.DimContextoCEPAL ALTER COLUMN contexto_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.DimTipoCampana ALTER COLUMN tipo_campana_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN atencion_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactCampanas ALTER COLUMN campana_key BIGINT NOT NULL;",

        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN fecha_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN centro_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN especialidad_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN canal_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN estado_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactAtenciones ALTER COLUMN contexto_key BIGINT NULL;",

        "ALTER TABLE dbo.FactCampanas ALTER COLUMN centro_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactCampanas ALTER COLUMN tipo_campana_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactCampanas ALTER COLUMN fecha_inicio_key BIGINT NOT NULL;",
        "ALTER TABLE dbo.FactCampanas ALTER COLUMN fecha_fin_key BIGINT NOT NULL;",

        # PRIMARY KEYS
        "ALTER TABLE dbo.DimTiempo ADD CONSTRAINT PK_DimTiempo PRIMARY KEY (fecha_key);",
        "ALTER TABLE dbo.DimCentroSalud ADD CONSTRAINT PK_DimCentroSalud PRIMARY KEY (centro_key);",
        "ALTER TABLE dbo.DimEspecialidad ADD CONSTRAINT PK_DimEspecialidad PRIMARY KEY (especialidad_key);",
        "ALTER TABLE dbo.DimCanal ADD CONSTRAINT PK_DimCanal PRIMARY KEY (canal_key);",
        "ALTER TABLE dbo.DimEstado ADD CONSTRAINT PK_DimEstado PRIMARY KEY (estado_key);",
        "ALTER TABLE dbo.DimContextoCEPAL ADD CONSTRAINT PK_DimContextoCEPAL PRIMARY KEY (contexto_key);",
        "ALTER TABLE dbo.DimTipoCampana ADD CONSTRAINT PK_DimTipoCampana PRIMARY KEY (tipo_campana_key);",
        "ALTER TABLE dbo.FactAtenciones ADD CONSTRAINT PK_FactAtenciones PRIMARY KEY (atencion_key);",
        "ALTER TABLE dbo.FactCampanas ADD CONSTRAINT PK_FactCampanas PRIMARY KEY (campana_key);",

        # FOREIGN KEYS
        """
        ALTER TABLE dbo.FactAtenciones
        ADD CONSTRAINT FK_FactAtenciones_DimTiempo
        FOREIGN KEY (fecha_key) REFERENCES dbo.DimTiempo(fecha_key);
        """,
        """
        ALTER TABLE dbo.FactAtenciones
        ADD CONSTRAINT FK_FactAtenciones_DimCentroSalud
        FOREIGN KEY (centro_key) REFERENCES dbo.DimCentroSalud(centro_key);
        """,
        """
        ALTER TABLE dbo.FactAtenciones
        ADD CONSTRAINT FK_FactAtenciones_DimEspecialidad
        FOREIGN KEY (especialidad_key) REFERENCES dbo.DimEspecialidad(especialidad_key);
        """,
        """
        ALTER TABLE dbo.FactAtenciones
        ADD CONSTRAINT FK_FactAtenciones_DimCanal
        FOREIGN KEY (canal_key) REFERENCES dbo.DimCanal(canal_key);
        """,
        """
        ALTER TABLE dbo.FactAtenciones
        ADD CONSTRAINT FK_FactAtenciones_DimEstado
        FOREIGN KEY (estado_key) REFERENCES dbo.DimEstado(estado_key);
        """,
        """
        ALTER TABLE dbo.FactAtenciones
        ADD CONSTRAINT FK_FactAtenciones_DimContextoCEPAL
        FOREIGN KEY (contexto_key) REFERENCES dbo.DimContextoCEPAL(contexto_key);
        """,
        """
        ALTER TABLE dbo.FactCampanas
        ADD CONSTRAINT FK_FactCampanas_DimCentroSalud
        FOREIGN KEY (centro_key) REFERENCES dbo.DimCentroSalud(centro_key);
        """,
        """
        ALTER TABLE dbo.FactCampanas
        ADD CONSTRAINT FK_FactCampanas_DimTipoCampana
        FOREIGN KEY (tipo_campana_key) REFERENCES dbo.DimTipoCampana(tipo_campana_key);
        """,
        """
        ALTER TABLE dbo.FactCampanas
        ADD CONSTRAINT FK_FactCampanas_DimTiempoInicio
        FOREIGN KEY (fecha_inicio_key) REFERENCES dbo.DimTiempo(fecha_key);
        """,
        """
        ALTER TABLE dbo.FactCampanas
        ADD CONSTRAINT FK_FactCampanas_DimTiempoFin
        FOREIGN KEY (fecha_fin_key) REFERENCES dbo.DimTiempo(fecha_key);
        """,

        # ÍNDICES
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'IX_FactAtenciones_fecha_key'
              AND object_id = OBJECT_ID('dbo.FactAtenciones')
        )
        CREATE INDEX IX_FactAtenciones_fecha_key ON dbo.FactAtenciones(fecha_key);
        """,
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'IX_FactAtenciones_centro_key'
              AND object_id = OBJECT_ID('dbo.FactAtenciones')
        )
        CREATE INDEX IX_FactAtenciones_centro_key ON dbo.FactAtenciones(centro_key);
        """,
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'IX_FactAtenciones_especialidad_key'
              AND object_id = OBJECT_ID('dbo.FactAtenciones')
        )
        CREATE INDEX IX_FactAtenciones_especialidad_key ON dbo.FactAtenciones(especialidad_key);
        """,
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'IX_FactAtenciones_estado_key'
              AND object_id = OBJECT_ID('dbo.FactAtenciones')
        )
        CREATE INDEX IX_FactAtenciones_estado_key ON dbo.FactAtenciones(estado_key);
        """,
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'IX_FactCampanas_centro_key'
              AND object_id = OBJECT_ID('dbo.FactCampanas')
        )
        CREATE INDEX IX_FactCampanas_centro_key ON dbo.FactCampanas(centro_key);
        """,
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'IX_FactCampanas_tipo_campana_key'
              AND object_id = OBJECT_ID('dbo.FactCampanas')
        )
        CREATE INDEX IX_FactCampanas_tipo_campana_key ON dbo.FactCampanas(tipo_campana_key);
        """
    ]

    with engine_gold.begin() as conn:
        for sentencia in sentencias:
            conn.execute(text(sentencia))

    print("Relaciones físicas Gold creadas correctamente.")


def crear_vistas_gold(engine_gold):
    vistas_sql = [
        """
        IF OBJECT_ID('dbo.vw_kpis_generales', 'V') IS NOT NULL
            DROP VIEW dbo.vw_kpis_generales;
        """,
        """
        CREATE VIEW dbo.vw_kpis_generales AS
        WITH SaturacionDiaria AS (
            SELECT
                FA.fecha_key,
                FA.centro_key,
                COUNT(*) AS total_fichas_dia,
                MAX(DC.capacidadDiaria) AS capacidad_diaria,
                CASE
                    WHEN MAX(DC.capacidadDiaria) = 0 THEN NULL
                    ELSE 100.0 * COUNT(*) / MAX(DC.capacidadDiaria)
                END AS indice_saturacion_pct
            FROM FactAtenciones FA
            INNER JOIN DimCentroSalud DC ON FA.centro_key = DC.centro_key
            GROUP BY FA.fecha_key, FA.centro_key
        )
        SELECT
            CAST(
                100.0 * SUM(FA.esAusente) /
                NULLIF(SUM(CASE WHEN DE.estado IN ('ATENDIDA','AUSENTE','CANCELADA') THEN 1 ELSE 0 END), 0)
                AS DECIMAL(10,2)
            ) AS tasa_ausentismo_pct,
            CAST(
                AVG(CASE WHEN FA.esAtendida = 1 THEN CAST(FA.tiempoEsperaMin AS FLOAT) END)
                AS DECIMAL(10,2)
            ) AS tiempo_promedio_espera_min,
            CAST(
                (SELECT AVG(indice_saturacion_pct) FROM SaturacionDiaria)
                AS DECIMAL(10,2)
            ) AS indice_saturacion_promedio_pct,
            SUM(FA.conteoFicha) AS total_fichas,
            SUM(FA.esAtendida) AS total_atendidas,
            SUM(FA.esAusente) AS total_ausentes,
            SUM(FA.esCancelada) AS total_canceladas,
            SUM(FA.esReservada) AS total_reservadas
        FROM FactAtenciones FA
        INNER JOIN DimEstado DE ON FA.estado_key = DE.estado_key;
        """,
        """
        IF OBJECT_ID('dbo.vw_saturacion_diaria', 'V') IS NOT NULL
            DROP VIEW dbo.vw_saturacion_diaria;
        """,
        """
        CREATE VIEW dbo.vw_saturacion_diaria AS
        SELECT
            DT.fecha,
            DT.anio,
            DT.mes,
            DT.nombre_mes,
            DC.departamento,
            DC.ciudad,
            DC.centroSalud,
            DC.capacidadDiaria,
            COUNT(*) AS total_fichas_dia,
            CAST(100.0 * COUNT(*) / NULLIF(MAX(DC.capacidadDiaria), 0) AS DECIMAL(10,2)) AS indice_saturacion_pct
        FROM FactAtenciones FA
        INNER JOIN DimTiempo DT ON FA.fecha_key = DT.fecha_key
        INNER JOIN DimCentroSalud DC ON FA.centro_key = DC.centro_key
        GROUP BY
            DT.fecha, DT.anio, DT.mes, DT.nombre_mes,
            DC.departamento, DC.ciudad, DC.centroSalud, DC.capacidadDiaria;
        """,
        """
        IF OBJECT_ID('dbo.vw_ausentismo_especialidad', 'V') IS NOT NULL
            DROP VIEW dbo.vw_ausentismo_especialidad;
        """,
        """
        CREATE VIEW dbo.vw_ausentismo_especialidad AS
        SELECT
            DEsp.especialidad,
            SUM(FA.conteoFicha) AS total_fichas,
            SUM(FA.esAusente) AS total_ausentes,
            CAST(100.0 * SUM(FA.esAusente) / NULLIF(SUM(FA.conteoFicha), 0) AS DECIMAL(10,2)) AS tasa_ausentismo_pct
        FROM FactAtenciones FA
        INNER JOIN DimEspecialidad DEsp ON FA.especialidad_key = DEsp.especialidad_key
        GROUP BY DEsp.especialidad;
        """,
        """
        IF OBJECT_ID('dbo.vw_serie_mensual', 'V') IS NOT NULL
            DROP VIEW dbo.vw_serie_mensual;
        """,
        """
        CREATE VIEW dbo.vw_serie_mensual AS
        SELECT
            DT.anio,
            DT.mes,
            DT.nombre_mes,
            COUNT(*) AS total_fichas,
            SUM(FA.esAtendida) AS total_atendidas,
            SUM(FA.esAusente) AS total_ausentes,
            SUM(FA.esCancelada) AS total_canceladas,
            CAST(AVG(CASE WHEN FA.esAtendida = 1 THEN CAST(FA.tiempoEsperaMin AS FLOAT) END) AS DECIMAL(10,2)) AS espera_promedio_min,
            MAX(DCP.valor_cepal) AS valor_cepal
        FROM FactAtenciones FA
        INNER JOIN DimTiempo DT ON FA.fecha_key = DT.fecha_key
        LEFT JOIN DimContextoCEPAL DCP ON FA.contexto_key = DCP.contexto_key
        GROUP BY DT.anio, DT.mes, DT.nombre_mes;
        """,
        """
        IF OBJECT_ID('dbo.vw_campanas_resumen', 'V') IS NOT NULL
            DROP VIEW dbo.vw_campanas_resumen;
        """,
        """
        CREATE VIEW dbo.vw_campanas_resumen AS
        SELECT
            DC.departamento,
            DC.ciudad,
            DC.centroSalud,
            DTC.tipo,
            FC.estado,
            COUNT(*) AS total_campanas,
            CAST(AVG(CAST(FC.duracionDias AS FLOAT)) AS DECIMAL(10,2)) AS duracion_promedio_dias
        FROM FactCampanas FC
        INNER JOIN DimCentroSalud DC ON FC.centro_key = DC.centro_key
        INNER JOIN DimTipoCampana DTC ON FC.tipo_campana_key = DTC.tipo_campana_key
        GROUP BY DC.departamento, DC.ciudad, DC.centroSalud, DTC.tipo, FC.estado;
        """
    ]

    with engine_gold.begin() as conn:
        for sentencia in vistas_sql:
            conn.execute(text(sentencia))

    print("Vistas Gold creadas correctamente.")


def validar_gold(engine_gold):
    consultas = {
        "DimTiempo": "SELECT COUNT(*) AS total FROM DimTiempo",
        "DimCentroSalud": "SELECT COUNT(*) AS total FROM DimCentroSalud",
        "DimEspecialidad": "SELECT COUNT(*) AS total FROM DimEspecialidad",
        "FactAtenciones": "SELECT COUNT(*) AS total FROM FactAtenciones",
        "FactCampanas": "SELECT COUNT(*) AS total FROM FactCampanas",
        "vw_kpis_generales": "SELECT * FROM vw_kpis_generales"
    }

    for nombre, query in consultas.items():
        df = pd.read_sql(query, engine_gold)
        print(f"Validación OK -> {nombre}")
        print(df.head())


if __name__ == "__main__":
    print("Iniciando proceso Gold...")

    crear_base_gold()
    fichas, campanas, cepal = cargar_silver()

    dim_tiempo = construir_dim_tiempo(fichas, campanas)
    dim_centro = construir_dim_centro_desde_bronze()
    dim_especialidad = construir_dim_especialidad(fichas)
    dim_canal = construir_dim_canal(fichas)
    dim_estado = construir_dim_estado(fichas)
    dim_cepal = construir_dim_cepal(cepal)
    dim_tipo = construir_dim_tipo_campana(campanas)

    fact_atenciones = construir_fact_atenciones(
        fichas, dim_tiempo, dim_centro, dim_especialidad, dim_canal, dim_estado, dim_cepal
    )
    fact_campanas = construir_fact_campanas(campanas, dim_tiempo, dim_centro, dim_tipo)

    validar_claves_modelo(fact_atenciones, fact_campanas)

    tablas = {
        "DimTiempo": dim_tiempo,
        "DimCentroSalud": dim_centro,
        "DimEspecialidad": dim_especialidad,
        "DimCanal": dim_canal,
        "DimEstado": dim_estado,
        "DimContextoCEPAL": dim_cepal,
        "DimTipoCampana": dim_tipo,
        "FactAtenciones": fact_atenciones,
        "FactCampanas": fact_campanas
    }

    exportar_csv_locales(tablas)
    engine_gold = cargar_tablas_gold_sql(tablas)
    crear_relaciones_gold(engine_gold)
    crear_vistas_gold(engine_gold)
    validar_gold(engine_gold)

    print("Proceso Gold completado correctamente.")