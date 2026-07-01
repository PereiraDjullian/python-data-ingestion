"""
etl.py
------
Módulo central do pipeline de ingestão — Zona Bronze.

Estrutura:
    Funções utilitárias  — json_to_dataframe, normalize_columns, save_csv
    BaseIngestor         — contrato abstrato com o ciclo completo do pipeline
    IBGEIngestor         — ingestor genérico para qualquer endpoint IBGE REST
    SIDRAIngestor        — ingestor genérico para qualquer tabela SIDRA
    Orchestrator         — coordena a execução de múltiplos ingestores

Fluxo por ingestor:
    extract()            → JSON da API → DataFrame bruto
    normalize_columns()  → nomes de colunas padronizados
    _add_audit_columns() → metadados de rastreabilidade
    _load()              → persiste no SQL Server via DatabaseManager
    save_csv()           → cópia local em CSV (opcional)
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, final

import pandas as pd
from sqlalchemy.engine import Engine

from config.config import (
    BRONZE_AUDIT_COLUMN_INGESTION_DATE,
    BRONZE_AUDIT_COLUMN_SOURCE,
    BRONZE_AUDIT_COLUMN_STATUS,
    CSV_OUTPUT_DIR,
    DB_SCHEMA,
)
from config.database import DatabaseManager, get_engine
from connectors.api_client import IBGEClient, SIDRAClient

logger = logging.getLogger(__name__)

__all__ = [
    "json_to_dataframe",
    "normalize_columns",
    "save_csv",
    "IngestorResult",
    "BaseIngestor",
    "IBGEIngestor",
    "SIDRAIngestor",
    "Orchestrator",
]


# ===========================================================================
# IngestorResult — resultado estruturado de cada execução
# ===========================================================================

@dataclass
class IngestorResult:
    """
    Captura o resultado completo de um único ingestor após run().

    Atributos:
        name         : nome da classe do ingestor (ex: "IBGEIngestor").
        table        : nome da tabela bronze de destino.
        success      : True se o pipeline foi concluído sem erros.
        rows_loaded  : número de linhas persistidas no SQL Server.
        csv_path     : caminho do arquivo CSV exportado ("" se não exportado).
        duration_sec : tempo de execução em segundos.
        error        : mensagem de erro (apenas quando success=False).
    """
    name: str
    table: str
    success: bool
    rows_loaded: int = field(default=0)
    csv_path: str = field(default="")
    duration_sec: float = field(default=0.0)
    error: str = field(default="")


# ===========================================================================
# Funções utilitárias — standalone e reutilizáveis
# ===========================================================================

def json_to_dataframe(
    data: list[dict[str, Any]] | dict[str, Any],
    sidra_format: bool = False,
) -> pd.DataFrame:
    """
    Converte o JSON retornado por qualquer endpoint do IBGE em DataFrame.

    Suporta dois formatos de resposta:

    **Formato IBGE REST** (lista de dicts planos ou aninhados):
        [{"id": 11, "sigla": "RO", "regiao": {"id": 1, "nome": "Norte"}}, ...]
        → pd.json_normalize() achata os campos aninhados automaticamente.
          Ex: "regiao.id", "regiao.nome".

    **Formato SIDRA** (primeira linha é cabeçalho de nomes):
        [{"D1C": "Grande Região", "V": "Valor"},   ← linha de cabeçalho
         {"D1C": "Norte",          "V": "9318"},    ← dado real
         ...]
        → O primeiro item define o mapeamento de chaves → nomes descritivos.
          Os demais itens são os registros reais, com colunas renomeadas.

    Args:
        data         : JSON decodificado retornado pela API (list ou dict).
        sidra_format : True para APIs do SIDRA; False para demais endpoints IBGE.

    Returns:
        pd.DataFrame: dados prontos para normalização e auditoria.
                      Retorna DataFrame vazio se `data` for vazio.
    """
    if not data:
        logger.warning("json_to_dataframe: dado de entrada vazio.")
        return pd.DataFrame()

    if sidra_format:
        if not isinstance(data, list) or len(data) < 2:
            logger.warning("json_to_dataframe (SIDRA): esperado list com >= 2 itens.")
            return pd.DataFrame()

        # Primeiro item: dict onde cada VALOR é o nome descritivo da coluna
        # Ex: {"D1C": "Grande Região", "V": "Valor"} → renomeia D1C → "Grande Região"
        col_map: dict[str, str] = {k: str(v) for k, v in data[0].items()}
        records = data[1:]  # Ignora a linha de cabeçalho

        df = pd.DataFrame(records).rename(columns=col_map)
        logger.debug("json_to_dataframe (SIDRA): %d registro(s), %d coluna(s).", len(df), len(df.columns))
        return df

    # Formato IBGE REST: achata dicionários aninhados com json_normalize
    raw = data if isinstance(data, list) else [data]
    df = pd.json_normalize(raw)
    logger.debug("json_to_dataframe (IBGE): %d registro(s), %d coluna(s).", len(df), len(df.columns))
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes de colunas para o padrão snake_case sem acentos.

    Transformações aplicadas (na ordem):
        1. Remove acentos via decomposição Unicode (NFD)
        2. Converte para minúsculas
        3. Substitui qualquer sequência de caracteres não alfanuméricos por "_"
        4. Remove underscores nas extremidades
        5. Colunas duplicadas recebem sufixo numérico (_1, _2 ...)

    Exemplos:
        "Nome do Município"  → "nome_do_municipio"
        "regiao.id"          → "regiao_id"
        "PIB (R$)"           → "pib_r"
        "Grande Região"      → "grande_regiao"

    Args:
        df: DataFrame com colunas a normalizar.

    Returns:
        pd.DataFrame: cópia do DataFrame com colunas padronizadas.
    """
    def _clean(name: str) -> str:
        # 1. Decompõe e remove marcas de acentuação (categoria Unicode "Mn")
        normalized = unicodedata.normalize("NFD", str(name))
        no_accents = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
        # 2. Minúsculas
        lower = no_accents.lower().strip()
        # 3. Substitui tudo que não for letra/dígito por underscore
        snake = re.sub(r"[^a-z0-9]+", "_", lower)
        # 4. Remove underscores nas pontas
        return snake.strip("_") or "col"

    df = df.copy()
    cleaned: list[str] = [_clean(c) for c in df.columns]

    # 5. Resolve duplicatas adicionando sufixo incremental
    seen: dict[str, int] = {}
    unique: list[str] = []
    for col in cleaned:
        if col in seen:
            seen[col] += 1
            unique.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique.append(col)

    df.columns = unique
    return df


def save_csv(
    df: pd.DataFrame,
    table_name: str,
    output_dir: str | None = None,
) -> Path:
    """
    Salva o DataFrame como arquivo CSV com timestamp no nome.

    Nome gerado: {table_name}_{YYYYMMDD_HHMMSS}UTC.csv
    Codificação: UTF-8 com BOM (utf-8-sig) para compatibilidade com Excel.

    Args:
        df         : DataFrame a exportar.
        table_name : nome base do arquivo (sem extensão).
        output_dir : diretório de destino. Usa CSV_OUTPUT_DIR do config se omitido.

    Returns:
        Path: caminho absoluto do arquivo gerado.

    Raises:
        OSError: se não for possível criar o diretório ou gravar o arquivo.
    """
    directory = Path(output_dir or CSV_OUTPUT_DIR)
    directory.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = directory / f"{table_name}_{timestamp}UTC.csv"

    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    logger.info("CSV exportado → %s (%d linha(s))", filepath, len(df))
    return filepath


# ===========================================================================
# BaseIngestor — contrato abstrato do pipeline
# ===========================================================================

class BaseIngestor(ABC):
    """
    Contrato de ingestão para a Zona Bronze.

    Subclasses concretas devem implementar apenas:
        - table_name (property) → nome da tabela de destino
        - extract()             → DataFrame com os dados brutos da API

    O método run() orquestra o ciclo completo e não deve ser sobrescrito
    (marcado com @final). A extensão é feita sobrescrevendo _source_url()
    ou os atributos de classe abaixo.

    Atributos configuráveis por subclasse:
        export_csv    (bool)                     : salva cópia CSV após carga.
        load_strategy ("append" | "replace")     : estratégia de persistência.
    """

    export_csv: bool = True
    load_strategy: Literal["append", "replace"] = "append"

    def __init__(self, engine: Engine | None = None) -> None:
        self._engine: Engine = engine or get_engine()

    # -- Interface obrigatória -----------------------------------------------

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Nome da tabela bronze de destino (sem schema)."""

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Consulta a API e retorna os dados brutos como DataFrame.
        Deve chamar json_to_dataframe() internamente.
        Não aplica regras de negócio.
        """

    # -- Pipeline principal (não sobrescrever) --------------------------------

    @final
    def run(self) -> "IngestorResult":
        """
        Executa o pipeline completo em 5 etapas e retorna métricas estruturadas.

            1. extract()            → DataFrame bruto da API
            2. normalize_columns()  → padroniza nomes de colunas
            3. _add_audit_columns() → adiciona metadados de rastreabilidade
            4. _load()              → persiste no SQL Server
            5. save_csv()           → exporta cópia CSV (se export_csv=True)

        Retorna IngestorResult com status, linhas carregadas, caminho CSV e duração.
        Exceções são capturadas e registradas sem propagar.
        """
        target = f"[{DB_SCHEMA}].[{self.table_name}]"
        logger.info("[%s] ▶ Iniciando ingestão → %s", self.__class__.__name__, target)
        start = time.monotonic()

        try:
            # Etapa 1 — Extração
            df = self.extract()
            if df.empty:
                logger.warning("[%s] API retornou dados vazios. Ingestão cancelada.", self.__class__.__name__)
                return IngestorResult(
                    name=self.__class__.__name__,
                    table=self.table_name,
                    success=False,
                    error="API retornou dados vazios.",
                    duration_sec=time.monotonic() - start,
                )
            logger.debug("[%s] extract: %d linha(s), %d coluna(s).", self.__class__.__name__, len(df), len(df.columns))

            # Etapa 2 — Normalização de colunas
            df = normalize_columns(df)

            # Etapa 3 — Colunas de auditoria
            df = self._add_audit_columns(df)

            # Etapa 4 — Persistência no SQL Server
            rows_loaded = self._load(df)
            logger.info(
                "[%s] ✔ %d registro(s) → %s",
                self.__class__.__name__, rows_loaded, target,
            )

            # Etapa 5 — Exportação CSV (opcional)
            csv_path = ""
            if self.export_csv:
                csv_path = str(save_csv(df, self.table_name))

            return IngestorResult(
                name=self.__class__.__name__,
                table=self.table_name,
                success=True,
                rows_loaded=rows_loaded,
                csv_path=csv_path,
                duration_sec=time.monotonic() - start,
            )

        except Exception as exc:
            logger.error(
                "[%s] ✘ Falha na ingestão: %s",
                self.__class__.__name__, exc, exc_info=True,
            )
            return IngestorResult(
                name=self.__class__.__name__,
                table=self.table_name,
                success=False,
                error=str(exc),
                duration_sec=time.monotonic() - start,
            )

    # -- Métodos internos (sobrescrevíveis) -----------------------------------

    def _add_audit_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona três colunas de auditoria ao DataFrame antes da carga.

            dt_ingestao  : timestamp UTC do momento da carga.
            origem_api   : URL consultada (via _source_url()).
            status_carga : sempre "SUCESSO" neste ponto; erros são capturados em run().
        """
        df = df.copy()
        # .replace(tzinfo=None) armazena UTC sem tzinfo. Datetime com timezone
        # é mapeado pelo pandas para TIMESTAMP (ROWVERSION) no SQL Server,
        # que não aceita inserção explícita. Naive datetime → DATETIME2.
        df[BRONZE_AUDIT_COLUMN_INGESTION_DATE] = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        df[BRONZE_AUDIT_COLUMN_SOURCE] = self._source_url()
        df[BRONZE_AUDIT_COLUMN_STATUS] = "SUCESSO"
        return df

    def _load(self, df: pd.DataFrame) -> int:
        """
        Persiste o DataFrame no SQL Server via DatabaseManager.

        Estratégias disponíveis (definidas pelo atributo load_strategy):
          "append"  → adiciona linhas. Cria a tabela se não existir.
          "replace" → TRUNCATE + INSERT. Preserva schema e índices.

        Returns:
            int: número de linhas persistidas.
        """
        with DatabaseManager(self._engine) as db:
            if self.load_strategy == "replace":
                return db.replace_dataframe(df, self.table_name)
            return db.insert_dataframe(df, self.table_name)

    def _source_url(self) -> str:
        """
        URL de origem consultada. Sobrescreva nas subclasses para rastreabilidade.
        """
        return "N/A"


# ===========================================================================
# IBGEIngestor — genérico para qualquer endpoint da API IBGE REST
# ===========================================================================

class IBGEIngestor(BaseIngestor):
    """
    Ingestor genérico para qualquer endpoint da API REST do IBGE.

    Em vez de criar uma subclasse por endpoint, este ingestor aceita
    uma função callable que recebe um IBGEClient e retorna os dados.
    Isso elimina duplicação de código para endpoints com estrutura similar.

    Args:
        table      : nome da tabela Bronze de destino.
        fetch_fn   : callable que recebe IBGEClient e retorna list[dict].
                     Exemplo: lambda client: client.get_estados()
        source_url : URL consultada (para auditoria).
        engine     : Engine SQLAlchemy opcional (usa Singleton se omitido).

    Exemplo de uso:
        from connectors.api_client import IBGEClient

        estados = IBGEIngestor(
            table="estados",
            fetch_fn=lambda c: c.get_estados(),
            source_url="https://servicodados.ibge.gov.br/api/v1/localidades/estados",
        )

        municipios_sp = IBGEIngestor(
            table="municipios_sp",
            fetch_fn=lambda c: c.get_municipios_por_uf(35),
            source_url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/35/municipios",
        )
    """

    def __init__(
        self,
        table: str,
        fetch_fn: Callable[[IBGEClient], list[dict[str, Any]]],
        source_url: str = "",
        engine: Engine | None = None,
    ) -> None:
        super().__init__(engine)
        self._table = table
        self._fetch_fn = fetch_fn
        self._url = source_url

    @property
    def table_name(self) -> str:
        return self._table

    def extract(self) -> pd.DataFrame:
        """
        Executa fetch_fn usando IBGEClient como context manager,
        garantindo que a Session HTTP seja sempre fechada ao final.

        Converte o retorno (list[dict]) em DataFrame via json_to_dataframe(),
        que achata automaticamente campos aninhados com pd.json_normalize().
        """
        logger.debug("[IBGEIngestor:%s] Abrindo IBGEClient...", self._table)
        with IBGEClient() as client:
            raw: list[dict[str, Any]] = self._fetch_fn(client)

        return json_to_dataframe(raw, sidra_format=False)

    def _source_url(self) -> str:
        return self._url


# ===========================================================================
# SIDRAIngestor — genérico para qualquer tabela do SIDRA
# ===========================================================================

class SIDRAIngestor(BaseIngestor):
    """
    Ingestor genérico para qualquer tabela da API SIDRA do IBGE.

    Todos os parâmetros da URL SIDRA são configuráveis no construtor,
    permitindo ingerir qualquer tabela sem criar novas subclasses.

    Args:
        table         : nome da tabela Bronze de destino.
        tabela        : código numérico da tabela SIDRA (ex: "1612").
        nivel         : nível territorial com prefixo n (ex: "n1", "n3", "n6").
                        Use SIDRAClient.NIVEIS para referência.
        localidade    : código IBGE da localidade ou "all" para todas.
        variavel      : código da variável ou "allxp" para todas sem percentual.
        periodo       : código do período ou "last" para o mais recente.
        classificacao : filtro de classificação opcional.
        engine        : Engine SQLAlchemy opcional.

    Exemplos:
        # População residente por estado — último censo
        SIDRAIngestor(
            table="populacao_por_estado",
            tabela="1612",
            nivel="n3",
        )

        # PIB municipal para todos os municípios
        SIDRAIngestor(
            table="pib_municipios",
            tabela="5938",
            nivel="n6",
            variavel="37",
        )
    """

    def __init__(
        self,
        table: str,
        tabela: str,
        nivel: str = "n1",
        localidade: str = "all",
        variavel: str = "allxp",
        periodo: str = "last",
        classificacao: str | None = None,
        engine: Engine | None = None,
    ) -> None:
        super().__init__(engine)
        self._table = table
        self._tabela = tabela
        self._nivel = nivel
        self._localidade = localidade
        self._variavel = variavel
        self._periodo = periodo
        self._classificacao = classificacao

    @property
    def table_name(self) -> str:
        return self._table

    def extract(self) -> pd.DataFrame:
        """
        Consulta a tabela SIDRA configurada e converte o resultado em DataFrame.

        A API SIDRA retorna a primeira linha como cabeçalho de nomes de colunas.
        json_to_dataframe(sidra_format=True) trata esse formato automaticamente:
            - Linha 0: mapeamento de chaves → nomes descritivos das colunas
            - Linhas 1+: registros reais com colunas renomeadas
        """
        logger.debug(
            "[SIDRAIngestor:%s] tabela=%s | nivel=%s | periodo=%s",
            self._table, self._tabela, self._nivel, self._periodo,
        )
        with SIDRAClient() as client:
            raw: list[dict[str, Any]] = client.get_tabela(
                tabela=self._tabela,
                nivel=self._nivel,
                localidade=self._localidade,
                variavel=self._variavel,
                periodo=self._periodo,
                classificacao=self._classificacao,
            )

        return json_to_dataframe(raw, sidra_format=True)

    def _source_url(self) -> str:
        """Reconstrói a URL SIDRA consultada para registro de auditoria."""
        from config.config import SIDRA_BASE_URL
        return (
            f"{SIDRA_BASE_URL}/values/t/{self._tabela}"
            f"/{self._nivel}/{self._localidade}"
            f"/v/{self._variavel}/p/{self._periodo}"
        )


# ===========================================================================
# Orchestrator — coordena a execução de todos os ingestores
# ===========================================================================

class Orchestrator:
    """
    Executa todos os ingestores registrados e consolida o resultado.

    Cada ingestor é executado de forma isolada: uma falha individual
    não interrompe os demais (tolerância a falhas por ingestor).

    Args:
        ingestors: lista de instâncias de BaseIngestor a executar.

    Exemplo:
        orchestrator = Orchestrator([
            IBGEIngestor("estados",   lambda c: c.get_estados()),
            IBGEIngestor("municipios", lambda c: c.get_todos_municipios()),
            SIDRAIngestor("populacao", tabela="1612", nivel="n3"),
        ])
        results = orchestrator.run_all()
    """

    def __init__(self, ingestors: list[BaseIngestor]) -> None:
        self._ingestors = ingestors

    def run_all(self) -> list["IngestorResult"]:
        """
        Executa todos os ingestores sequencialmente.

        Registra o início e o fim de cada ingestor no log.
        Captura exceções individuais sem propagar para o chamador.

        Returns:
            list[IngestorResult]: resultado de cada ingestor, na mesma
                                  ordem em que foram registrados.
        """
        total = len(self._ingestors)
        results: list[IngestorResult] = []

        logger.info("=" * 55)
        logger.info(" Pipeline Bronze iniciado | %d ingestor(es)", total)
        logger.info("=" * 55)

        for idx, ingestor in enumerate(self._ingestors, start=1):
            name = ingestor.__class__.__name__
            logger.info("[%d/%d] Executando %s...", idx, total, name)
            result = ingestor.run()
            results.append(result)

        succeeded = sum(1 for r in results if r.success)
        failed = [r.table for r in results if not r.success]

        logger.info("=" * 55)
        logger.info(
            " Pipeline Bronze finalizado | %d/%d com sucesso", succeeded, total
        )
        if failed:
            logger.warning(" Falhas: %s", ", ".join(failed))
        logger.info("=" * 55)

        return results
