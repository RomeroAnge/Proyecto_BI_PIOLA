from sqlalchemy import create_engine
from urllib.parse import quote_plus

SERVER = r"ROMEROANGELO\MSSQLSERVER2025"
DRIVER = "ODBC Driver 17 for SQL Server"


def crear_engine(database="BI_SaludPublicaTarija_Bronze"):
    cadena_odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(cadena_odbc)}"
    )