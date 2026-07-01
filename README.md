# Bronze Zone — Ingestão de Dados IBGE/SIDRA

Pipeline de ingestão de dados brutos (Zona Bronze) consumindo APIs públicas do IBGE e SIDRA, com persistência no SQL Server.

---

## Arquitetura

```
bronze_zone/
├── config/
│   ├── config.py          # Configurações globais (lidas de .env)
│   └── database.py        # Engine SQLAlchemy (Singleton) + helpers de DB
├── connectors/
│   └── api_client.py      # Clientes HTTP IBGE e SIDRA (requests + retries)
├── pipeline/
│   └── etl.py             # BaseIngestor (contrato ABC) + Orchestrator
├── sql/                   # DDL das tabelas bronze (create_bronze_tables.sql)
├── data/csv/              # CSVs exportados em runtime (gerado automaticamente)
├── logs/                  # Logs gerados em runtime
├── logger.py              # Configuração centralizada de logging
├── main.py                # Ponto de entrada do pipeline
├── .env                   # Variáveis de ambiente (não versionado)
└── requirements.txt
```

---

## Pré-requisitos

| Requisito | Versão mínima |
|---|---|
| Python | 3.11 |
| ODBC Driver for SQL Server | 17 ou 18 |
| SQL Server | 2019+ / Azure SQL |

---

## Instalação

```bash
# 1. Criar e ativar ambiente virtual
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Linux/macOS

# 2. Instalar dependências
pip install -r requirements.txt
```

---

## Configuração

Crie um arquivo `.env` na raiz do projeto `bronze_zone/`:

```env
# API
IBGE_BASE_URL=https://servicodados.ibge.gov.br/api/v1
SIDRA_BASE_URL=https://apisidra.ibge.gov.br
REQUEST_TIMEOUT=30
REQUEST_MAX_RETRIES=3

# SQL Server — Autenticação Windows
DB_SERVER=localhost
DB_NAME=faculdade_db
DB_SCHEMA=bronze
DB_DRIVER=ODBC Driver 17 for SQL Server
DB_TRUSTED_CONNECTION=true

# SQL Server — Autenticação SQL (alternativa)
# DB_TRUSTED_CONNECTION=false
# DB_USER=seu_usuario
# DB_PASSWORD=sua_senha

# Logging
LOG_LEVEL=INFO
```

---

## Execução

```bash
cd bronze_zone
python main.py
```

O pipeline cria o banco `faculdade_db` e o schema `bronze` automaticamente se não existirem.

---

## Fluxo do Pipeline

```
main.py
  └── ensure_database()       # Cria banco faculdade_db se não existir
  └── test_connection()       # Valida conexão SQL Server
  └── ensure_schema()         # Cria schema bronze se não existir
  └── Orchestrator.run_all()
        └── [Para cada ingestor]
              ├── extract()             # Consulta API → DataFrame bruto
              ├── _add_audit_columns()  # dt_ingestao, origem_api, status_carga
              └── _load()               # DataFrame → SQL Server (append)
                                        # + exporta CSV em data/csv/
```

---

## Tabelas Ingeridas

| Tabela | Fonte | Registros | Descrição |
|---|---|---|---|
| `bronze.estados` | IBGE REST | 27 | UFs com sigla, nome e região |
| `bronze.municipios` | IBGE REST | 5.571 | Municípios do Brasil |
| `bronze.populacao_por_estado` | SIDRA 1612 | 135 | Pop. residente por estado/sexo/cor |
| `bronze.bronze_pib` | SIDRA 5938 | 27 | PIB a preços correntes por estado |
| `bronze.bronze_analfabetismo` | SIDRA 1378 | 27 | Taxa de analfabetismo — Censo 2010 |
| `bronze.bronze_densidade` | SIDRA 1301 | 54 | Área e densidade demográfica |
| `bronze.bronze_saneamento` | SIDRA 1393 | 27 | Domicílios por tipo de esgotamento |
| `bronze.saude_mortalidade_infantil` | SIDRA 2612 | 27 | Mortalidade infantil por estado |
| `bronze.saude_esperanca_vida` | SIDRA 3175 | 27 | Esperança de vida ao nascer |
| `bronze.saude_plano_saude` | SIDRA 3543 | 27 | Cobertura de serviços de saúde |

---

## Adicionando um Novo Ingestor

Basta adicionar uma entrada em `main.py` → `build_ingestors()`. Não é necessário criar arquivos extras.

**Para uma tabela do SIDRA** (mais comum):
```python
# Em main.py, dentro de build_ingestors():
SIDRAIngestor(
    table="nome_da_tabela",
    tabela="CODIGO_SIDRA",  # código numérico da tabela
    nivel="n3",             # n1=Brasil, n2=Região, n3=Estado, n6=Município
)
```

**Para um endpoint REST do IBGE:**
```python
IBGEIngestor(
    table="nome_da_tabela",
    fetch_fn=lambda c: c.get_estados(),  # método do IBGEClient
    source_url="https://url-para-auditoria",
)
```

Para descobrir códigos SIDRA: https://sidra.ibge.gov.br

---

## Tabelas Bronze — Padrão de Colunas

Toda tabela no schema `bronze` inclui as colunas de auditoria:

| Coluna | Tipo | Descrição |
|---|---|---|
| `dt_ingestao` | DATETIME2 | Timestamp UTC da carga |
| `origem_api` | VARCHAR | URL consultada |
| `status_carga` | VARCHAR | `SUCESSO` / `ERRO` |

---

## Tecnologias

- **Python 3.11+**
- **requests** — cliente HTTP com retry automático
- **pandas** — manipulação de DataFrames
- **SQLAlchemy 2.x** — ORM/Core para SQL Server
- **pyodbc** — driver ODBC para SQL Server
- **python-dotenv** — gerenciamento de variáveis de ambiente

