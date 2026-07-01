"""
config.py
---------
Centraliza todas as configurações do projeto.
Lê variáveis de ambiente via os.getenv com fallback para valores padrão.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# API IBGE / SIDRA
# ---------------------------------------------------------------------------
IBGE_BASE_URL: str = os.getenv("IBGE_BASE_URL", "https://servicodados.ibge.gov.br/api/v1")
SIDRA_BASE_URL: str = os.getenv("SIDRA_BASE_URL", "https://apisidra.ibge.gov.br")

REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
REQUEST_MAX_RETRIES: int = int(os.getenv("REQUEST_MAX_RETRIES", "3"))
REQUEST_BACKOFF_FACTOR: float = float(os.getenv("REQUEST_BACKOFF_FACTOR", "0.5"))

# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------
DB_SERVER: str = os.getenv("DB_SERVER", "localhost")
DB_NAME: str = os.getenv("DB_NAME", "faculdade_db")
DB_SCHEMA: str = os.getenv("DB_SCHEMA", "bronze")
DB_DRIVER: str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

# Autenticação Windows (Trusted Connection) ou SQL Server
DB_TRUSTED_CONNECTION: bool = os.getenv("DB_TRUSTED_CONNECTION", "true").lower() == "true"
DB_USER: str = os.getenv("DB_USER", "")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR: str = os.getenv("LOG_DIR", "logs")
LOG_FILENAME: str = os.getenv("LOG_FILENAME", "app.log")
LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))  # 5 MB
LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
BRONZE_AUDIT_COLUMN_INGESTION_DATE: str = "dt_ingestao"
BRONZE_AUDIT_COLUMN_SOURCE: str = "origem_api"
BRONZE_AUDIT_COLUMN_STATUS: str = "status_carga"

# Diretório onde as cópias CSV serão salvas
CSV_OUTPUT_DIR: str = os.getenv("CSV_OUTPUT_DIR", "data/csv")
