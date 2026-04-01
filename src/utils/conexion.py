from sqlalchemy import create_engine
from urllib.parse import quote_plus

SERVER = r"ROMEROANGELO\MSSQLSERVER2025"
DATABASE = "BI_SaludPublicaTarija_Bronze"
DRIVER = "ODBC Driver 17 for SQL Server"

def crear_engine():
    cadena_odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(cadena_odbc)}"
    )
    return engine