"""
logger.py
---------
Configura o sistema de logging centralizado do projeto.
Utiliza RotatingFileHandler para controle de tamanho de arquivo
e StreamHandler para saída no console.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys

from config.config import (
    LOG_BACKUP_COUNT,
    LOG_DIR,
    LOG_FILENAME,
    LOG_LEVEL,
    LOG_MAX_BYTES,
)

__all__ = ["setup_logging"]

_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)-35s | %(message)s"
)
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging() -> None:
    """
    Configura o logger raiz com dois handlers:
      - RotatingFileHandler → grava em logs/app.log com rotação automática.
      - StreamHandler       → exibe no console (stdout).

    Deve ser chamado uma única vez, em main.py, antes de qualquer operação.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # -- Handler: arquivo rotativo ------------------------------------------
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(LOG_DIR, LOG_FILENAME),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    # -- Handler: console ---------------------------------------------------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    # -- Logger raiz ---------------------------------------------------------
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Evita adicionar handlers duplicados em recargas
    if not root_logger.handlers:
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    # Silencia libs verbosas
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
