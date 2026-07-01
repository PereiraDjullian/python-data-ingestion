"""
api_client.py
-------------
Clientes HTTP para as APIs públicas do IBGE e SIDRA.

Hierarquia de classes:
    BaseAPIClient          — Session, retry, timeout, logging, exceções
    ├── IBGEClient         — /localidades, /agregados
    └── SIDRAClient        — /values (tabelas SIDRA parametrizadas)

Exceções próprias:
    APIClientError         — base de todas as exceções deste módulo
    ├── APIHTTPError       — resposta HTTP com status de erro (4xx / 5xx)
    ├── APITimeoutError    — timeout atingido após os retries
    └── APIConnectionError — falha de rede / DNS
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.config import (
    IBGE_BASE_URL,
    REQUEST_BACKOFF_FACTOR,
    REQUEST_MAX_RETRIES,
    REQUEST_TIMEOUT,
    SIDRA_BASE_URL,
)

logger = logging.getLogger(__name__)

__all__ = [
    "APIClientError",
    "APIHTTPError",
    "APITimeoutError",
    "APIConnectionError",
    "IBGEClient",
    "SIDRAClient",
]


# ---------------------------------------------------------------------------
# Hierarquia de exceções
# ---------------------------------------------------------------------------

class APIClientError(Exception):
    """Base para todas as exceções de clientes HTTP deste projeto."""


class APIHTTPError(APIClientError):
    """
    Lançada quando o servidor retorna um status de erro HTTP (4xx ou 5xx).

    Atributos:
        status_code : código HTTP retornado (ex: 404, 500).
        url         : URL que causou o erro.
    """

    def __init__(self, status_code: int, url: str, message: str = "") -> None:
        self.status_code = status_code
        self.url = url
        super().__init__(message or f"HTTP {status_code} ao acessar {url}")


class APITimeoutError(APIClientError):
    """Lançada quando o timeout é atingido, mesmo após os retries."""


class APIConnectionError(APIClientError):
    """Lançada quando não é possível estabelecer conexão (rede, DNS)."""


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class BaseAPIClient(ABC):
    """
    Classe base para clientes HTTP do projeto.

    Gerencia:
      - Ciclo de vida da requests.Session
      - Política de retry com backoff exponencial
      - Timeout por requisição
      - Logging estruturado de cada chamada
      - Conversão de exceções do requests em APIClientError

    Uso como context manager (recomendado):
        with IBGEClient() as client:
            data = client.get_estados()
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session: requests.Session = self._build_session()
        logger.debug("%s inicializado | base_url=%s", self.__class__.__name__, self.base_url)

    # -- Session -------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        """
        Cria e configura a requests.Session com política de retry.

        Política de retry:
          - total            : número máximo de tentativas (REQUEST_MAX_RETRIES).
          - backoff_factor   : fator de espera entre tentativas. Com fator 0.5,
                               as esperas são 0s, 0.5s, 1s, 2s, 4s...
          - status_forcelist : códigos HTTP que disparam retry automático.
                               429 = rate limit; 5xx = erros do servidor.
          - allowed_methods  : apenas GET é idempotente e seguro para retry.

        Returns:
            requests.Session configurada e pronta para uso.
        """
        session = requests.Session()

        retry_policy = Retry(
            total=REQUEST_MAX_RETRIES,
            backoff_factor=REQUEST_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "bronze-zone-pipeline/1.0",
        })

        return session

    # -- Método de requisição ------------------------------------------------

    def _get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        full_url: str | None = None,
    ) -> Any:
        """
        Executa uma requisição GET autenticada e retorna o JSON decodificado.

        Aceita `full_url` para URLs já montadas externamente (usado pelo
        SIDRAClient, que constrói URLs com caminhos parametrizados no path).

        Fluxo:
            1. Monta a URL (endpoint relativo ou full_url)
            2. Executa GET com timeout
            3. Loga status e tempo de resposta
            4. Verifica status HTTP (raise_for_status)
            5. Decodifica e retorna o JSON
            6. Converte exceções do requests em APIClientError

        Args:
            endpoint : caminho relativo à base_url (ex: "/localidades/estados").
            params   : query string como dicionário (ex: {"view": "nivelado"}).
            full_url : URL completa. Quando informada, `endpoint` é ignorado.

        Returns:
            Any: conteúdo JSON decodificado (dict, list, etc.).

        Raises:
            APIHTTPError       : status HTTP de erro após todos os retries.
            APITimeoutError    : timeout atingido.
            APIConnectionError : falha de rede.
            APIClientError     : demais erros inesperados.
        """
        url = full_url or f"{self.base_url}/{endpoint.lstrip('/')}"
        start = time.monotonic()

        logger.debug("→ GET %s | params=%s", url, params)

        try:
            response = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            elapsed = time.monotonic() - start
            logger.debug(
                "← %d %s | %.2fs | %s",
                response.status_code,
                response.reason,
                elapsed,
                url,
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            logger.error("HTTP %d ao acessar %s", status, url)
            raise APIHTTPError(status_code=status, url=url) from exc

        except requests.exceptions.Timeout as exc:
            logger.error("Timeout (>%ds) ao acessar %s", REQUEST_TIMEOUT, url)
            raise APITimeoutError(f"Timeout após {REQUEST_TIMEOUT}s: {url}") from exc

        except requests.exceptions.ConnectionError as exc:
            logger.error("Falha de conexão ao acessar %s: %s", url, exc)
            raise APIConnectionError(f"Falha de conexão: {url}") from exc

        except requests.exceptions.RequestException as exc:
            logger.error("Erro inesperado na requisição para %s: %s", url, exc)
            raise APIClientError(f"Erro inesperado: {url}") from exc

    # -- Context Manager -----------------------------------------------------

    def __enter__(self) -> "BaseAPIClient":
        """Suporte a `with Client() as c:`."""
        return self

    def __exit__(self, *_: Any) -> None:
        """Garante o fechamento da Session ao sair do bloco `with`."""
        self.close()

    def close(self) -> None:
        """
        Encerra a Session HTTP e libera conexões do pool.
        Chamar explicitamente quando não usar context manager.
        """
        self._session.close()
        logger.debug("%s: Session encerrada.", self.__class__.__name__)

    # -- Contrato ------------------------------------------------------------

    @abstractmethod
    def health_check(self) -> bool:
        """
        Verifica se a API está acessível.

        Returns:
            bool: True se a API responde corretamente, False caso contrário.
        """


# ---------------------------------------------------------------------------
# IBGE Client
# ---------------------------------------------------------------------------

class IBGEClient(BaseAPIClient):
    """
    Cliente para a API REST do IBGE.

    Documentação oficial:
        https://servicodados.ibge.gov.br/api/docs

    Endpoints implementados:
        GET /localidades/estados                   → lista de UFs
        GET /localidades/estados/{uf}/municipios   → municípios de uma UF
        GET /localidades/municipios                → todos os municípios do Brasil
        GET /agregados                             → catálogo de pesquisas do IBGE
        GET /agregados/{id}/periodos               → períodos disponíveis
    """

    def __init__(self) -> None:
        super().__init__(IBGE_BASE_URL)

    # -- Health Check --------------------------------------------------------

    def health_check(self) -> bool:
        """
        Verifica disponibilidade da API consultando /localidades/estados.
        Retorna True se ao menos um estado for retornado, False caso contrário.
        """
        try:
            data = self._get("/localidades/estados")
            ok = isinstance(data, list) and len(data) > 0
            logger.info("IBGE health_check → %s", "OK" if ok else "FALHA")
            return ok
        except APIClientError as exc:
            logger.error("IBGE health_check falhou: %s", exc)
            return False

    # -- Localidades ---------------------------------------------------------

    def get_estados(self) -> list[dict[str, Any]]:
        """
        Retorna a lista de todos os estados (UFs) do Brasil.

        Endpoint: GET /localidades/estados

        Returns:
            list[dict]: cada item contém id, sigla, nome e região.

        Exemplo de retorno:
            [{"id": 11, "sigla": "RO", "nome": "Rondônia", "regiao": {...}}, ...]
        """
        logger.info("IBGEClient: buscando estados...")
        data: list[dict[str, Any]] = self._get("/localidades/estados")
        logger.info("IBGEClient: %d estado(s) retornado(s).", len(data))
        return data

    def get_municipios_por_uf(self, uf_id: int | str) -> list[dict[str, Any]]:
        """
        Retorna todos os municípios de uma Unidade Federativa.

        Endpoint: GET /localidades/estados/{uf}/municipios

        Args:
            uf_id: código numérico do IBGE (ex: 35 para SP) ou sigla (ex: "SP").

        Returns:
            list[dict]: cada item contém id, nome, microrregião e mesorregião.
        """
        logger.info("IBGEClient: buscando municípios da UF '%s'...", uf_id)
        data: list[dict[str, Any]] = self._get(f"/localidades/estados/{uf_id}/municipios")
        logger.info("IBGEClient: %d município(s) retornado(s) para UF '%s'.", len(data), uf_id)
        return data

    def get_todos_municipios(self) -> list[dict[str, Any]]:
        """
        Retorna todos os municípios do Brasil em uma única chamada.

        Endpoint: GET /localidades/municipios

        Returns:
            list[dict]: lista completa de municípios (~5.570 itens).
        """
        logger.info("IBGEClient: buscando todos os municípios do Brasil...")
        data: list[dict[str, Any]] = self._get("/localidades/municipios")
        logger.info("IBGEClient: %d município(s) retornado(s).", len(data))
        return data

    # -- Agregados -----------------------------------------------------------

    def get_agregados(self) -> list[dict[str, Any]]:
        """
        Retorna o catálogo de pesquisas (agregados) disponíveis no IBGE.

        Endpoint: GET /agregados

        Returns:
            list[dict]: lista de agregados com id e nome da pesquisa.
        """
        logger.info("IBGEClient: buscando catálogo de agregados...")
        data: list[dict[str, Any]] = self._get("/agregados")
        logger.info("IBGEClient: %d agregado(s) retornado(s).", len(data))
        return data

    def get_periodos_agregado(self, agregado_id: int | str) -> list[dict[str, Any]]:
        """
        Retorna os períodos disponíveis para um agregado específico.

        Endpoint: GET /agregados/{agregado}/periodos

        Args:
            agregado_id: código numérico do agregado (ex: 1301 para PNAD).

        Returns:
            list[dict]: lista de períodos com id e literalPeriodo.
        """
        logger.info("IBGEClient: buscando períodos do agregado '%s'...", agregado_id)
        data: list[dict[str, Any]] = self._get(f"/agregados/{agregado_id}/periodos")
        logger.info(
            "IBGEClient: %d período(s) retornado(s) para agregado '%s'.",
            len(data), agregado_id,
        )
        return data


# ---------------------------------------------------------------------------
# SIDRA Client
# ---------------------------------------------------------------------------

class SIDRAClient(BaseAPIClient):
    """
    Cliente para a API SIDRA do IBGE (Sistema IBGE de Recuperação Automática).

    A URL do SIDRA é montada como path (não query string), seguindo o padrão:
        /values/t/{tabela}/n{nivel}/{localidade}/v/{variavel}/p/{periodo}

    Documentação oficial:
        http://api.sidra.ibge.gov.br/

    Endpoints implementados:
        get_tabela()  — consulta genérica e flexível por tabela SIDRA
    """

    # Mapeamento de níveis territoriais para facilitar uso
    NIVEIS = {
        "brasil": "n1",
        "regiao": "n2",
        "estado": "n3",
        "municipio": "n6",
        "mesorregiao": "n7",
        "microrregiao": "n8",
    }

    def __init__(self) -> None:
        super().__init__(SIDRA_BASE_URL)

    # -- Health Check --------------------------------------------------------

    def health_check(self) -> bool:
        """
        Verifica disponibilidade da API SIDRA consultando a tabela 1612
        (Censo Demográfico — população por UF), limitada ao nível Brasil.

        Returns:
            bool: True se a API responde com dados, False caso contrário.
        """
        try:
            data = self.get_tabela(
                tabela="1612",
                nivel="n1",
                localidade="all",
                variavel="allxp",
                periodo="last",
            )
            ok = isinstance(data, list) and len(data) > 0
            logger.info("SIDRA health_check → %s", "OK" if ok else "FALHA")
            return ok
        except APIClientError as exc:
            logger.error("SIDRA health_check falhou: %s", exc)
            return False

    # -- Construção de URL ---------------------------------------------------

    def _build_sidra_url(
        self,
        tabela: str,
        nivel: str,
        localidade: str,
        variavel: str,
        periodo: str,
        classificacao: str | None = None,
    ) -> str:
        """
        Monta a URL completa de consulta ao SIDRA conforme especificação oficial.

        Estrutura da URL:
            /values/t/{tabela}/{nivel}/{localidade}/v/{variavel}/p/{periodo}
            [/{classificacao}]

        Os parâmetros /d/ (decimais) e /h/ (cabeçalho) são omitidos
        intencionalmente: o SIDRA exige número inteiro em /d/ e "y"/"n" em /h/.
        Omitir usa os padrões da API (cabeçalho incluso, decimais da variável).

        Args:
            tabela       : código numérico da tabela SIDRA (ex: "1612").
            nivel        : nível territorial com prefixo n (ex: "n1", "n3").
            localidade   : código da localidade ou "all" para todas.
            variavel     : código da variável, "all" ou "allxp" (sem percentual).
            periodo      : período ou "last" para o mais recente.
            classificacao: segmento extra de classificação opcional.

        Returns:
            str: URL completa pronta para requisição.
        """
        path = f"/values/t/{tabela}/{nivel}/{localidade}/v/{variavel}/p/{periodo}"

        if classificacao:
            path += f"/{classificacao}"

        url = f"{self.base_url}{path}"
        logger.debug("SIDRA URL construída: %s", url)
        return url

    # -- Consulta principal --------------------------------------------------

    def get_tabela(
        self,
        tabela: str,
        nivel: str = "n1",
        localidade: str = "all",
        variavel: str = "allxp",
        periodo: str = "last",
        classificacao: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Consulta genérica a uma tabela do SIDRA.

        Parâmetros principais:
            tabela      : código da tabela (ex: "1612" = Censo Pop. Residente).
            nivel       : nível territorial. Use SIDRAClient.NIVEIS para referência
                          ou informe diretamente "n1", "n3", "n6", etc.
            localidade  : código IBGE da localidade ou "all" para todas.
            variavel    : código da variável ou "allxp" para todas sem percentual.
            periodo     : código do período ou "last" para o mais recente.
            classificacao: filtro de classificação (opcional).

        Returns:
            list[dict]: lista de registros retornados pela API. O primeiro item
                        geralmente é o cabeçalho das colunas.

        Raises:
            APIHTTPError       : resposta com status de erro.
            APITimeoutError    : timeout excedido.
            APIConnectionError : falha de rede.

        Exemplos:
            # População total do Brasil — último censo
            client.get_tabela("1612", nivel="n1")

            # PIB municipal para todos os municípios do último período
            client.get_tabela("5938", nivel="n6", variavel="37", periodo="last")
        """
        url = self._build_sidra_url(
            tabela=tabela,
            nivel=nivel,
            localidade=localidade,
            variavel=variavel,
            periodo=periodo,
            classificacao=classificacao,
        )

        logger.info(
            "SIDRAClient: consultando tabela=%s | nivel=%s | localidade=%s | periodo=%s",
            tabela, nivel, localidade, periodo,
        )

        data: list[dict[str, Any]] = self._get(full_url=url, endpoint="")

        # O SIDRA retorna o cabeçalho como primeiro item da lista
        registros = len(data) - 1 if len(data) > 1 else 0
        logger.info(
            "SIDRAClient: tabela=%s retornou %d registro(s).", tabela, registros
        )

        return data
