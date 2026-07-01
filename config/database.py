"""
database.py
-----------
Módulo responsável por toda a infraestrutura de banco de dados do projeto.

Responsabilidades:
  - Criar o banco de dados caso não exista
  - Gerenciar a Engine SQLAlchemy (Singleton via lru_cache)
  - Criar e validar o schema Bronze
  - Expor um DatabaseManager (context manager) para operações de escrita
  - Inserir e substituir DataFrames do pandas no SQL Server

Uso típico:
    with DatabaseManager() as db:
        db.insert_dataframe(df, "minha_tabela")
        db.replace_dataframe(df, "outra_tabela")
"""

from __future__ import annotations

import logging
import urllib
from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from config.config import (
    DB_DRIVER,
    DB_NAME,
    DB_PASSWORD,
    DB_SCHEMA,
    DB_SERVER,
    DB_TRUSTED_CONNECTION,
    DB_USER,
)

logger = logging.getLogger(__name__)

__all__ = [
    "get_engine",
    "ensure_schema",
    "ensure_database",
    "test_connection",
    "DatabaseManager",
]


# ---------------------------------------------------------------------------
# Construção da connection string
# ---------------------------------------------------------------------------

def _build_odbc_params(database: str = DB_NAME) -> str:
    """
    Monta os parâmetros ODBC em formato de string URL-encoded.

    O argumento `database` permite apontar para 'master' ao criar o banco,
    e para DB_NAME no uso normal do pipeline.

    Returns:
        str: parâmetros ODBC codificados para uso na URL do SQLAlchemy.
    """
    params: dict[str, str] = {
        "DRIVER": f"{{{DB_DRIVER}}}",
        "SERVER": DB_SERVER,
        "DATABASE": database,
    }

    if DB_TRUSTED_CONNECTION:
        params["Trusted_Connection"] = "yes"
    else:
        if not DB_USER or not DB_PASSWORD:
            raise ValueError(
                "DB_USER e DB_PASSWORD são obrigatórios quando "
                "DB_TRUSTED_CONNECTION=false."
            )
        params["UID"] = DB_USER
        params["PWD"] = DB_PASSWORD

    odbc_str = ";".join(f"{k}={v}" for k, v in params.items())
    return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)


# ---------------------------------------------------------------------------
# Criação automática do banco de dados
# ---------------------------------------------------------------------------

def ensure_database() -> None:
    """
    Cria o banco de dados no SQL Server caso ele ainda não exista.

    Estratégia: conecta ao banco 'master' (sempre disponível) e executa
    um CREATE DATABASE condicional. Usa AUTOCOMMIT porque DDL de banco
    não pode rodar dentro de uma transação no SQL Server.

    Deve ser chamada uma única vez, antes de get_engine().
    """
    master_url = _build_odbc_params(database="master")

    # isolation_level="AUTOCOMMIT" é obrigatório para CREATE DATABASE
    master_engine = create_engine(
        master_url,
        isolation_level="AUTOCOMMIT",
        pool_pre_ping=True,
    )

    try:
        with master_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT COUNT(1) FROM sys.databases WHERE name = :dbname"),
                {"dbname": DB_NAME},
            ).scalar()

            if not exists:
                # Identificador entre colchetes previne SQL Injection no nome do banco
                conn.execute(text(f"CREATE DATABASE [{DB_NAME}]"))
                logger.info("Banco de dados '%s' criado com sucesso.", DB_NAME)
            else:
                logger.debug("Banco de dados '%s' já existe. Nenhuma ação necessária.", DB_NAME)

    except SQLAlchemyError as exc:
        logger.error("Erro ao verificar/criar o banco de dados '%s': %s", DB_NAME, exc)
        raise
    finally:
        master_engine.dispose()


# ---------------------------------------------------------------------------
# Engine Singleton
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Retorna a Engine SQLAlchemy como Singleton.

    O decorador @lru_cache garante que apenas uma instância seja criada
    por processo, evitando múltiplas conexões desnecessárias ao pool.

    Configurações relevantes:
      - fast_executemany=True : ativa o modo batch do pyodbc, acelerando
                                inserções de múltiplas linhas.
      - pool_pre_ping=True    : executa um SELECT 1 antes de entregar uma
                                conexão do pool, descartando conexões mortas.
      - pool_size=5           : número de conexões mantidas abertas.
      - max_overflow=10       : conexões extras permitidas além do pool_size.

    Returns:
        Engine: instância configurada e pronta para uso.
    """
    connection_url = _build_odbc_params(database=DB_NAME)

    engine = create_engine(
        connection_url,
        fast_executemany=True,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        echo=False,
    )

    logger.debug(
        "Engine criada | servidor=%s | banco=%s | schema=%s",
        DB_SERVER, DB_NAME, DB_SCHEMA,
    )
    return engine


# ---------------------------------------------------------------------------
# Schema Bronze
# ---------------------------------------------------------------------------

def ensure_schema(engine: Engine | None = None) -> None:
    """
    Cria o schema Bronze no banco de dados caso não exista.

    O schema Bronze é o namespace que isola as tabelas de dados brutos
    das demais (ex: dbo, silver, gold). Deve ser chamado na inicialização
    do pipeline, após ensure_database().

    Args:
        engine: Engine opcional. Usa o Singleton se não informado.
    """
    engine = engine or get_engine()

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :schema) "
                    "EXEC('CREATE SCHEMA [' + :schema + ']')"
                ),
                {"schema": DB_SCHEMA},
            )
        logger.info("Schema '%s' verificado/criado com sucesso.", DB_SCHEMA)

    except SQLAlchemyError as exc:
        logger.error("Erro ao criar o schema '%s': %s", DB_SCHEMA, exc)
        raise


# ---------------------------------------------------------------------------
# Verificação de conexão
# ---------------------------------------------------------------------------

def test_connection(engine: Engine | None = None) -> bool:
    """
    Verifica se a Engine consegue estabelecer uma conexão ativa.

    Executa um SELECT 1 simples. Útil na inicialização do pipeline para
    identificar problemas de rede ou credenciais antes de processar dados.

    Args:
        engine: Engine opcional. Usa o Singleton se não informado.

    Returns:
        bool: True se a conexão está funcional, False caso contrário.
    """
    engine = engine or get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexão com SQL Server verificada com sucesso.")
        return True
    except SQLAlchemyError as exc:
        logger.error("Falha ao conectar ao SQL Server: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Context Manager de conexão interna
# ---------------------------------------------------------------------------

@contextmanager
def _managed_connection(engine: Engine) -> Generator[Connection, None, None]:
    """
    Context manager interno que abre uma conexão transacional.

    Faz commit automático ao final do bloco `with` e rollback em caso de
    exceção, garantindo atomicidade nas operações de escrita.

    Yields:
        Connection: conexão ativa dentro de uma transação.
    """
    with engine.begin() as conn:
        yield conn


# ---------------------------------------------------------------------------
# DatabaseManager — interface principal de escrita
# ---------------------------------------------------------------------------

class DatabaseManager:
    """
    Interface de alto nível para operações de banco de dados.

    Deve ser usada como context manager para garantir que os recursos
    (conexões, transações) sejam liberados corretamente.

    Exemplo:
        with DatabaseManager() as db:
            db.insert_dataframe(df, "populacao")
            db.replace_dataframe(df_novo, "pib")

    Args:
        engine: Engine opcional. Usa o Singleton se não informado.
    """

    def __init__(self, engine: Engine | None = None) -> None:
        self._engine: Engine = engine or get_engine()

    # -- Context Manager -----------------------------------------------------

    def __enter__(self) -> "DatabaseManager":
        """Abre o DatabaseManager e valida a conexão."""
        logger.debug("DatabaseManager iniciado.")
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object) -> bool:
        """
        Encerra o DatabaseManager.
        Não suprime exceções (retorna False), apenas registra no log.
        """
        if exc_type is not None:
            logger.error(
                "DatabaseManager encerrado com erro: %s — %s", exc_type.__name__, exc_val
            )
        else:
            logger.debug("DatabaseManager encerrado normalmente.")
        return False  # Propaga a exceção original

    # -- Consultas auxiliares ------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        """
        Verifica se uma tabela existe no schema Bronze.

        Usa o SQLAlchemy Inspector, que consulta os metadados do banco sem
        executar queries manuais, tornando o código portável.

        Args:
            table_name: nome da tabela (sem o schema).

        Returns:
            bool: True se a tabela existe no schema Bronze.
        """
        inspector = inspect(self._engine)
        exists = inspector.has_table(table_name, schema=DB_SCHEMA)
        logger.debug(
            "table_exists('%s.%s') → %s", DB_SCHEMA, table_name, exists
        )
        return exists

    def get_row_count(self, table_name: str) -> int:
        """
        Retorna o total de linhas de uma tabela no schema Bronze.

        Útil para validação pós-carga: confirmar que os registros foram
        persistidos corretamente.

        Args:
            table_name: nome da tabela (sem o schema).

        Returns:
            int: número de linhas na tabela.
        """
        with _managed_connection(self._engine) as conn:
            result = conn.execute(
                text(f"SELECT COUNT(1) FROM [{DB_SCHEMA}].[{table_name}]")
            ).scalar()
        count = int(result or 0)
        logger.debug("get_row_count('%s.%s') → %d", DB_SCHEMA, table_name, count)
        return count

    # -- Operações de escrita ------------------------------------------------

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        chunksize: int = 1000,
    ) -> int:
        """
        Insere um DataFrame no SQL Server usando a estratégia APPEND.

        Preserva todos os dados históricos: cada execução adiciona novas
        linhas sem remover as existentes. Se a tabela não existir, ela
        é criada automaticamente pelo pandas/SQLAlchemy com base nos
        tipos do DataFrame.

        Args:
            df        : DataFrame com os dados a inserir.
            table_name: nome da tabela de destino (sem schema).
            chunksize : número de linhas por batch de inserção.

        Returns:
            int: número de linhas inseridas.

        Raises:
            SQLAlchemyError: em caso de falha na inserção.
            ValueError: se o DataFrame estiver vazio.
        """
        if df.empty:
            logger.warning("insert_dataframe: DataFrame vazio. Nenhum dado inserido em '%s'.", table_name)
            return 0

        try:
            df.to_sql(
                name=table_name,
                con=self._engine,
                schema=DB_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=chunksize,
            )
            logger.info(
                "INSERT | %d linha(s) → [%s].[%s]",
                len(df), DB_SCHEMA, table_name,
            )
            return len(df)

        except SQLAlchemyError as exc:
            logger.error(
                "Erro no INSERT em '[%s].[%s]': %s", DB_SCHEMA, table_name, exc
            )
            raise

    def replace_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        chunksize: int = 1000,
    ) -> int:
        """
        Substitui os dados de uma tabela usando a estratégia TRUNCATE + INSERT.

        Diferente de `if_exists="replace"` do pandas (que faz DROP + CREATE
        e destrói índices e constraints), esta abordagem apenas limpa as
        linhas e reinserve os novos dados, preservando a estrutura da tabela.

        Se a tabela não existir, ela será criada automaticamente.

        Fluxo:
            1. Se a tabela existe → TRUNCATE TABLE
            2. INSERT das novas linhas (append na tabela agora vazia)

        Args:
            df        : DataFrame com os dados substitutos.
            table_name: nome da tabela de destino (sem schema).
            chunksize : número de linhas por batch de inserção.

        Returns:
            int: número de linhas inseridas após a substituição.

        Raises:
            SQLAlchemyError: em caso de falha no truncate ou na inserção.
            ValueError: se o DataFrame estiver vazio.
        """
        if df.empty:
            logger.warning("replace_dataframe: DataFrame vazio. Operação cancelada em '%s'.", table_name)
            return 0

        try:
            if self.table_exists(table_name):
                with _managed_connection(self._engine) as conn:
                    conn.execute(text(f"TRUNCATE TABLE [{DB_SCHEMA}].[{table_name}]"))
                logger.debug("TRUNCATE TABLE [%s].[%s] executado.", DB_SCHEMA, table_name)

            df.to_sql(
                name=table_name,
                con=self._engine,
                schema=DB_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=chunksize,
            )
            logger.info(
                "REPLACE (TRUNCATE+INSERT) | %d linha(s) → [%s].[%s]",
                len(df), DB_SCHEMA, table_name,
            )
            return len(df)

        except SQLAlchemyError as exc:
            logger.error(
                "Erro no REPLACE em '[%s].[%s]': %s", DB_SCHEMA, table_name, exc
            )
            raise

    def execute_ddl(self, sql: str) -> None:
        """
        Executa uma instrução DDL arbitrária no banco de dados.

        Útil para criação manual de tabelas com tipos específicos,
        criação de índices ou constraints que o pandas não gera
        automaticamente.

        Args:
            sql: instrução DDL completa (ex: CREATE TABLE, CREATE INDEX).

        Raises:
            SQLAlchemyError: se a instrução DDL falhar.
        """
        try:
            with _managed_connection(self._engine) as conn:
                conn.execute(text(sql))
            logger.debug("DDL executado com sucesso.")
        except SQLAlchemyError as exc:
            logger.error("Erro ao executar DDL: %s | SQL: %.200s", exc, sql)
            raise
