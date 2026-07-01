"""
main.py
-------
Ponto de entrada do pipeline Bronze Zone.

Fluxo completo:
    1. Configurar logging
    2. Pre-flight: criar banco, verificar conexão, garantir schema Bronze
    3. Registrar ingestores
    4. Executar pipeline via Orchestrator
    5. Gerar e registrar relatório de execução
    6. Retornar código de saída

Para adicionar uma nova fonte de dados:
    → Adicione uma nova instância em build_ingestors().
      Use IBGEIngestor para endpoints REST ou SIDRAIngestor para tabelas SIDRA.
    → Nenhuma outra mudança é necessária no restante do código.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone

from config.config import DB_NAME, DB_SCHEMA, DB_SERVER
from config.database import ensure_database, ensure_schema, get_engine, test_connection
from logger import setup_logging
from pipeline.etl import IBGEIngestor, IngestorResult, Orchestrator, SIDRAIngestor

# ---------------------------------------------------------------------------
# Setup de logging — deve ser o primeiro passo
# ---------------------------------------------------------------------------
setup_logging()
logger = logging.getLogger(__name__)


def build_ingestors() -> list:
    """
    Declara e retorna todos os ingestores que o pipeline deve executar.

    Como adicionar uma nova API:
        IBGEIngestor — qualquer endpoint da API REST do IBGE:
            IBGEIngestor(
                table="nome_tabela_bronze",
                fetch_fn=lambda c: c.metodo_do_cliente(),
                source_url="https://url-para-auditoria",
            )

        SIDRAIngestor — qualquer tabela do SIDRA:
            SIDRAIngestor(
                table="nome_tabela_bronze",
                tabela="codigo_tabela_sidra",
                nivel="n3",  # n1=Brasil, n2=Região, n3=Estado, n6=Município
            )

    Endpoints disponíveis no IBGEClient:
        get_estados()              → 27 UFs com sigla, nome e região
        get_todos_municipios()     → ~5.570 municípios do Brasil
        get_municipios_por_uf(id)  → municípios de uma UF específica
        get_agregados()            → catálogo de pesquisas do IBGE
        get_periodos_agregado(id)  → períodos de um agregado

    Exemplos adicionais (descomente para ativar):
        IBGEIngestor(
            table="municipios_sp",
            fetch_fn=lambda c: c.get_municipios_por_uf(35),
            source_url=".../localidades/estados/35/municipios",
        ),
        SIDRAIngestor(table="pib_municipios",  tabela="5938", nivel="n6"),
        SIDRAIngestor(table="populacao_total", tabela="1612", nivel="n1"),
    """
    return [
        # ------------------------------------------------------------------
        # Localidades — API REST IBGE
        # ------------------------------------------------------------------
        IBGEIngestor(
            table="estados",
            fetch_fn=lambda c: c.get_estados(),
            source_url=(
                "https://servicodados.ibge.gov.br"
                "/api/v1/localidades/estados"
            ),
        ),
        IBGEIngestor(
            table="municipios",
            fetch_fn=lambda c: c.get_todos_municipios(),
            source_url=(
                "https://servicodados.ibge.gov.br"
                "/api/v1/localidades/municipios"
            ),
        ),
        # ------------------------------------------------------------------
        # Dados demográficos — SIDRA
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="populacao_por_estado",
            tabela="1612",
            nivel="n3",   # n3 = estados
        ),
        # ------------------------------------------------------------------
        # PIB — SIDRA Tabela 5938
        # Produto Interno Bruto a preços correntes por estado (n3)
        # Fonte: IBGE — Contas Regionais do Brasil
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="bronze_pib",
            tabela="5938",
            nivel="n3",
            variavel="37",   # Variável 37 = PIB a preços correntes (R$ mil)
        ),
        # ------------------------------------------------------------------
        # Analfabetismo — SIDRA Tabela 1378
        # Pessoas de 10 anos ou mais, total e alfabetizadas — Censo 2010
        # Nível estadual (n3)
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="bronze_analfabetismo",
            tabela="1378",
            nivel="n3",
        ),
        # ------------------------------------------------------------------
        # Densidade demográfica — SIDRA Tabela 1301
        # Área territorial, população residente e densidade — Censo 2010
        # Nível estadual (n3)
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="bronze_densidade",
            tabela="1301",
            nivel="n3",
        ),
        # ------------------------------------------------------------------
        # Saneamento básico — SIDRA Tabela 1393
        # Domicílios particulares permanentes por tipo de esgotamento
        # sanitário — Censo 2010, nível estadual (n3)
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="bronze_saneamento",
            tabela="1393",
            nivel="n3",
        ),

        # ------------------------------------------------------------------
        # Saúde — SIDRA
        # ------------------------------------------------------------------
        # Mortalidade infantil — Taxa de mortalidade de menores de 1 ano
        # Fonte: IBGE — Censo Demográfico 2010 (Tabela 2612)
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="saude_mortalidade_infantil",
            tabela="2612",
            nivel="n3",
        ),
        # ------------------------------------------------------------------
        # Esperança de vida ao nascer por UF
        # Fonte: IBGE — Tábuas de Mortalidade (Tabela 3175)
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="saude_esperanca_vida",
            tabela="3175",
            nivel="n3",
        ),
        # ------------------------------------------------------------------
        # Cobertura de plano de saúde por UF — PNAD Contínua (Tabela 7165)
        # ------------------------------------------------------------------
        SIDRAIngestor(
            table="saude_plano_saude",
            tabela="3543",   # ← era 7165 (inválido), agora 3543
            nivel="n3",
        ),
    ]


# ===========================================================================
# Pre-flight — verificações de infraestrutura antes do pipeline
# ===========================================================================

def _run_preflight() -> bool:
    """
    Executa as verificações de infraestrutura necessárias antes do pipeline.

    Sequência:
        1. Cria o banco de dados caso não exista (conecta ao master)
        2. Testa a conexão ativa com o banco configurado
        3. Cria o schema Bronze caso não exista

    Returns:
        bool: True se tudo está operacional, False em caso de qualquer falha.
              Falhas são registradas como CRITICAL no log.
    """
    logger.info("--- Pre-flight | servidor=%s | banco=%s ---", DB_SERVER, DB_NAME)

    # 1. Garantir existência do banco de dados
    try:
        ensure_database()
    except Exception as exc:
        logger.critical("Pre-flight FALHOU ao criar banco '%s': %s", DB_NAME, exc)
        return False

    # 2. Verificar conectividade
    engine = get_engine()
    if not test_connection(engine):
        logger.critical(
            "Pre-flight FALHOU: sem conexão com '%s/%s'. "
            "Verifique servidor, credenciais e driver ODBC.",
            DB_SERVER, DB_NAME,
        )
        return False

    logger.info("Pre-flight: conexão com SQL Server verificada.")

    # 3. Garantir schema Bronze
    try:
        ensure_schema(engine)
        logger.info("Pre-flight: schema '%s' verificado/criado.", DB_SCHEMA)
    except Exception as exc:
        logger.critical("Pre-flight FALHOU ao criar schema '%s': %s", DB_SCHEMA, exc)
        return False

    logger.info("--- Pre-flight concluído com sucesso ---")
    return True


# ===========================================================================
# Relatório de execução
# ===========================================================================

def _build_report(results: list[IngestorResult], pipeline_start: float) -> str:
    """
    Gera um relatório tabular ASCII com o resultado de cada ingestor.

    Colunas: Tabela | Status | Linhas | Duração
    Rodapé : totais consolidados (ingestores, linhas, tempo total).

    Args:
        results        : lista de IngestorResult retornada pelo Orchestrator.
        pipeline_start : timestamp do início do pipeline (time.monotonic()).

    Returns:
        str: relatório completo pronto para ser registrado no log.
    """
    total_elapsed = time.monotonic() - pipeline_start
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Larguras das colunas (incluindo espaços internos)
    W_TABLE  = 26
    W_STATUS = 8
    W_ROWS   = 9
    W_DUR    = 11
    W_LINE   = W_TABLE + W_STATUS + W_ROWS + W_DUR + 5

    sep = f"+{'-' * W_TABLE}+{'-' * W_STATUS}+{'-' * W_ROWS}+{'-' * W_DUR}+"

    def _row(table: str, status: str, rows: str, dur: str) -> str:
        """Formata uma linha da tabela com truncamento automático."""
        t = (table[: W_TABLE - 3] + "...") if len(table) > W_TABLE - 2 else table
        return (
            f"| {t:<{W_TABLE - 2}} "
            f"| {status:<{W_STATUS - 2}} "
            f"| {rows:>{W_ROWS - 2}} "
            f"| {dur:>{W_DUR - 2}} |"
        )

    lines: list[str] = [
        "",
        "=" * W_LINE,
        "  RELATORIO DE EXECUCAO  —  Bronze Zone Pipeline",
        f"  Data/Hora    : {ts}",
        f"  Servidor     : {DB_SERVER}",
        f"  Banco/Schema : {DB_NAME} / {DB_SCHEMA}",
        "=" * W_LINE,
        sep,
        _row("Tabela", "Status", "Linhas", "Duracao"),
        sep,
    ]

    for r in results:
        status = "OK"    if r.success else "FALHA"
        rows   = str(r.rows_loaded) if r.success else "-"
        dur    = f"{r.duration_sec:.2f}s"
        lines.append(_row(r.table, status, rows, dur))

        if r.csv_path:
            lines.append(f"|   CSV: ...{os.sep}{os.path.basename(r.csv_path)}")
        if r.error:
            err_short = (r.error[:68] + "...") if len(r.error) > 68 else r.error
            lines.append(f"|   Erro: {err_short}")

    lines.append(sep)

    total_ingestors = len(results)
    ok              = sum(1 for r in results if r.success)
    falha           = total_ingestors - ok
    total_rows      = sum(r.rows_loaded for r in results)

    lines += [
        f"  Ingestores : {total_ingestors} total  |  {ok} OK  |  {falha} FALHA",
        f"  Linhas     : {total_rows} inseridas no SQL Server",
        f"  Tempo total: {total_elapsed:.2f}s",
        "=" * W_LINE,
        "",
    ]

    return "\n".join(lines)


# ===========================================================================
# Ponto de entrada principal
# ===========================================================================

def main() -> int:
    """
    Orquestra o pipeline completo da Zona Bronze.

    Fluxo:
        Conectar ao SQL Server → Consumir APIs → Converter para DataFrame
        → Inserir na Bronze → Salvar CSV → Gerar relatório

    Códigos de saída:
        0 — sucesso completo (todos os ingestores OK)
        1 — falha no pre-flight (banco/conexão indisponível)
        2 — pipeline executou, mas um ou mais ingestores falharam
    """
    pipeline_start = time.monotonic()

    logger.info("=" * 60)
    logger.info(
        " Bronze Zone Pipeline | %s",
        datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    )
    logger.info("=" * 60)

    # -- Etapa 1: Pre-flight -------------------------------------------------
    if not _run_preflight():
        logger.critical("Pipeline abortado na fase de pre-flight.")
        return 1

    # -- Etapa 2: Registrar ingestores ----------------------------------------
    ingestors = build_ingestors()

    if not ingestors:
        logger.warning(
            "Nenhum ingestor registrado em build_ingestors(). "
            "Adicione instâncias de IBGEIngestor ou SIDRAIngestor."
        )
        return 0

    logger.info("%d ingestor(es) registrado(s) para execução.", len(ingestors))

    # -- Etapa 3: Executar pipeline ------------------------------------------
    orchestrator = Orchestrator(ingestors)
    results: list[IngestorResult] = orchestrator.run_all()

    # -- Etapa 4: Gerar e registrar relatório --------------------------------
    report = _build_report(results, pipeline_start)
    logger.info(report)

    # -- Etapa 5: Código de saída --------------------------------------------
    failures = [r for r in results if not r.success]
    if failures:
        logger.warning(
            "%d ingestor(es) com falha: %s",
            len(failures),
            ", ".join(r.table for r in failures),
        )
        return 2

    logger.info("Pipeline concluído com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
