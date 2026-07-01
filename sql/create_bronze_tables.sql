-- =============================================================================
-- Script  : create_bronze_tables.sql
-- Banco   : faculdade_db
-- Schema  : bronze
-- Objetivo: Criar as tabelas da Zona Bronze para ingestão de dados IBGE/SIDRA
--
-- Execução: sqlcmd -S localhost -d faculdade_db -E -i create_bronze_tables.sql
-- =============================================================================

USE faculdade_db;
GO

-- -----------------------------------------------------------------------------
-- Garantir existência do schema bronze
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA [bronze]');
GO


-- =============================================================================
-- 1. bronze.bronze_pib
-- Origem  : SIDRA — Tabela 5938 (PIB dos Municípios a Preços Correntes)
-- Nível   : Municipal (n6) e Estadual (n3)
-- Variável: Produto Interno Bruto a preços correntes (R$ mil)
-- =============================================================================
IF OBJECT_ID('bronze.bronze_pib', 'U') IS NOT NULL
    PRINT 'Tabela bronze.bronze_pib já existe — ignorando criação.';
ELSE
BEGIN
    CREATE TABLE bronze.bronze_pib (

        -- -------------------------------------------------------------------
        -- Chave primária surrogate — gerada automaticamente pelo SQL Server.
        -- IDENTITY(1,1): começa em 1, incrementa de 1 em 1.
        -- Surrogate key garante unicidade independente dos dados da API,
        -- que podem chegar incompletos ou sem identificador natural único.
        -- -------------------------------------------------------------------
        id_bronze           INT             NOT NULL  IDENTITY(1,1),

        -- -------------------------------------------------------------------
        -- Dados territoriais
        -- VARCHAR ao invés de INT/NUMERIC porque a Bronze Zone preserva os
        -- valores exatamente como a API os retorna, sem coerção de tipo.
        -- Códigos IBGE como "3550308" parecem numéricos mas são tratados
        -- como string pela API e podem ter zeros à esquerda em versões futuras.
        -- -------------------------------------------------------------------
        codigo_territorio   VARCHAR(20)     NULL,
        nome_territorio     VARCHAR(150)    NULL,
        nivel_territorial   VARCHAR(50)     NULL,   -- Ex: "Município", "Estado"
        sigla_uf            VARCHAR(2)      NULL,

        -- -------------------------------------------------------------------
        -- Dados da variável SIDRA
        -- -------------------------------------------------------------------
        codigo_variavel     VARCHAR(20)     NULL,
        nome_variavel       VARCHAR(200)    NULL,   -- Ex: "Produto Interno Bruto"
        unidade_medida      VARCHAR(50)     NULL,   -- Ex: "Mil Reais"
        valor               VARCHAR(50)     NULL,   -- Mantido como VARCHAR: pode
                                                    -- conter "-", "X" (sigilo)
                                                    -- ou "..." (dado inexistente)
        periodo             VARCHAR(10)     NULL,   -- Ex: "2021", "2022"

        -- -------------------------------------------------------------------
        -- Colunas de auditoria — padrão de toda tabela Bronze.
        -- Permitem rastrear quando e de onde cada registro foi importado.
        -- -------------------------------------------------------------------
        origem_api          VARCHAR(500)    NULL,
        status_carga        VARCHAR(20)     NULL,
        data_importacao     DATETIME2(0)    NOT NULL  DEFAULT SYSUTCDATETIME(),

        -- -------------------------------------------------------------------
        -- Restrições
        -- -------------------------------------------------------------------
        CONSTRAINT PK_bronze_pib PRIMARY KEY CLUSTERED (id_bronze)
    );

    PRINT 'Tabela bronze.bronze_pib criada com sucesso.';
END
GO


-- =============================================================================
-- 2. bronze.bronze_analfabetismo
-- Origem  : SIDRA — Tabela 1378 (Taxa de analfabetismo)
--           Censo Demográfico / PNAD Contínua
-- Nível   : Municipal (n6), Estadual (n3), Nacional (n1)
-- Variável: Taxa de analfabetismo por faixa etária e situação do domicílio
-- =============================================================================
IF OBJECT_ID('bronze.bronze_analfabetismo', 'U') IS NOT NULL
    PRINT 'Tabela bronze.bronze_analfabetismo já existe — ignorando criação.';
ELSE
BEGIN
    CREATE TABLE bronze.bronze_analfabetismo (

        id_bronze           INT             NOT NULL  IDENTITY(1,1),

        -- Dados territoriais
        codigo_territorio   VARCHAR(20)     NULL,
        nome_territorio     VARCHAR(150)    NULL,
        nivel_territorial   VARCHAR(50)     NULL,
        sigla_uf            VARCHAR(2)      NULL,

        -- Classificações SIDRA
        -- A taxa de analfabetismo é segmentada por faixa etária e situação
        -- do domicílio (urbano/rural). Esses campos refletem as
        -- classificações retornadas diretamente pela API, sem enum.
        faixa_etaria        VARCHAR(50)     NULL,   -- Ex: "15 anos ou mais"
        situacao_domicilio  VARCHAR(30)     NULL,   -- Ex: "Urbana", "Rural", "Total"

        -- Dados da variável
        codigo_variavel     VARCHAR(20)     NULL,
        nome_variavel       VARCHAR(200)    NULL,
        unidade_medida      VARCHAR(50)     NULL,   -- Ex: "%"
        valor               VARCHAR(50)     NULL,   -- Taxa percentual como string
        periodo             VARCHAR(10)     NULL,

        -- Auditoria
        origem_api          VARCHAR(500)    NULL,
        status_carga        VARCHAR(20)     NULL,
        data_importacao     DATETIME2(0)    NOT NULL  DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_bronze_analfabetismo PRIMARY KEY CLUSTERED (id_bronze)
    );

    PRINT 'Tabela bronze.bronze_analfabetismo criada com sucesso.';
END
GO


-- =============================================================================
-- 3. bronze.bronze_densidade
-- Origem  : SIDRA — Tabela 1301 (Densidade demográfica)
--           Censo Demográfico — área territorial e população residente
-- Nível   : Municipal (n6), Estadual (n3)
-- Variáveis: Área (km²), População residente, Densidade (hab/km²)
-- =============================================================================
IF OBJECT_ID('bronze.bronze_densidade', 'U') IS NOT NULL
    PRINT 'Tabela bronze.bronze_densidade já existe — ignorando criação.';
ELSE
BEGIN
    CREATE TABLE bronze.bronze_densidade (

        id_bronze               INT             NOT NULL  IDENTITY(1,1),

        -- Dados territoriais
        codigo_territorio       VARCHAR(20)     NULL,
        nome_territorio         VARCHAR(150)    NULL,
        nivel_territorial       VARCHAR(50)     NULL,
        sigla_uf                VARCHAR(2)      NULL,

        -- Dados de área e população
        -- Armazenados como VARCHAR para preservar a string original da API.
        -- A conversão para FLOAT/INT ocorrerá nas camadas Silver/Gold.
        area_km2                VARCHAR(30)     NULL,   -- Área territorial em km²
        populacao_residente     VARCHAR(30)     NULL,   -- Total de habitantes
        densidade_demografica   VARCHAR(30)     NULL,   -- hab/km² (pode ter decimais)

        -- Dados da variável
        codigo_variavel         VARCHAR(20)     NULL,
        nome_variavel           VARCHAR(200)    NULL,
        unidade_medida          VARCHAR(50)     NULL,
        valor                   VARCHAR(50)     NULL,
        periodo                 VARCHAR(10)     NULL,

        -- Auditoria
        origem_api              VARCHAR(500)    NULL,
        status_carga            VARCHAR(20)     NULL,
        data_importacao         DATETIME2(0)    NOT NULL  DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_bronze_densidade PRIMARY KEY CLUSTERED (id_bronze)
    );

    PRINT 'Tabela bronze.bronze_densidade criada com sucesso.';
END
GO


-- =============================================================================
-- 4. bronze.bronze_saneamento
-- Origem  : SIDRA — Tabela 1393 (Domicílios e moradores por tipo de saneamento)
--           Censo Demográfico — abastecimento de água, esgoto e coleta de lixo
-- Nível   : Municipal (n6), Estadual (n3)
-- Variáveis: Domicílios com/sem acesso a serviços de saneamento básico
-- =============================================================================
IF OBJECT_ID('bronze.bronze_saneamento', 'U') IS NOT NULL
    PRINT 'Tabela bronze.bronze_saneamento já existe — ignorando criação.';
ELSE
BEGIN
    CREATE TABLE bronze.bronze_saneamento (

        id_bronze               INT             NOT NULL  IDENTITY(1,1),

        -- Dados territoriais
        codigo_territorio       VARCHAR(20)     NULL,
        nome_territorio         VARCHAR(150)    NULL,
        nivel_territorial       VARCHAR(50)     NULL,
        sigla_uf                VARCHAR(2)      NULL,

        -- Classificações de saneamento
        -- O Censo organiza saneamento em múltiplas dimensões.
        -- Esses campos capturam as classificações retornadas pela API
        -- sem impor validações (responsabilidade das camadas superiores).
        tipo_saneamento         VARCHAR(100)    NULL,   -- Ex: "Rede geral", "Fossa séptica"
        situacao_domicilio      VARCHAR(30)     NULL,   -- "Urbana", "Rural", "Total"
        tipo_domicilio          VARCHAR(50)     NULL,   -- "Particular permanente", etc.

        -- Dados da variável
        codigo_variavel         VARCHAR(20)     NULL,
        nome_variavel           VARCHAR(200)    NULL,
        unidade_medida          VARCHAR(50)     NULL,   -- Ex: "Domicílios", "%"
        valor                   VARCHAR(50)     NULL,
        periodo                 VARCHAR(10)     NULL,

        -- Auditoria
        origem_api              VARCHAR(500)    NULL,
        status_carga            VARCHAR(20)     NULL,
        data_importacao         DATETIME2(0)    NOT NULL  DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_bronze_saneamento PRIMARY KEY CLUSTERED (id_bronze)
    );

    PRINT 'Tabela bronze.bronze_saneamento criada com sucesso.';
END
GO


-- =============================================================================
-- Índices de suporte — acesso por código territorial e período
-- Não são únicos porque a Bronze Zone aceita cargas repetidas (histórico).
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_pib')
      AND name = 'IX_bronze_pib_territorio_periodo'
)
    CREATE NONCLUSTERED INDEX IX_bronze_pib_territorio_periodo
        ON bronze.bronze_pib (codigo_territorio, periodo);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_analfabetismo')
      AND name = 'IX_bronze_analfabetismo_territorio_periodo'
)
    CREATE NONCLUSTERED INDEX IX_bronze_analfabetismo_territorio_periodo
        ON bronze.bronze_analfabetismo (codigo_territorio, periodo);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_densidade')
      AND name = 'IX_bronze_densidade_territorio_periodo'
)
    CREATE NONCLUSTERED INDEX IX_bronze_densidade_territorio_periodo
        ON bronze.bronze_densidade (codigo_territorio, periodo);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_saneamento')
      AND name = 'IX_bronze_saneamento_territorio_periodo'
)
    CREATE NONCLUSTERED INDEX IX_bronze_saneamento_territorio_periodo
        ON bronze.bronze_saneamento (codigo_territorio, periodo);
GO


-- =============================================================================
-- Índice em data_importacao — útil para consultas de auditoria e monitoramento
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_pib')
      AND name = 'IX_bronze_pib_data_importacao'
)
    CREATE NONCLUSTERED INDEX IX_bronze_pib_data_importacao
        ON bronze.bronze_pib (data_importacao DESC);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_analfabetismo')
      AND name = 'IX_bronze_analfabetismo_data_importacao'
)
    CREATE NONCLUSTERED INDEX IX_bronze_analfabetismo_data_importacao
        ON bronze.bronze_analfabetismo (data_importacao DESC);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_densidade')
      AND name = 'IX_bronze_densidade_data_importacao'
)
    CREATE NONCLUSTERED INDEX IX_bronze_densidade_data_importacao
        ON bronze.bronze_densidade (data_importacao DESC);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('bronze.bronze_saneamento')
      AND name = 'IX_bronze_saneamento_data_importacao'
)
    CREATE NONCLUSTERED INDEX IX_bronze_saneamento_data_importacao
        ON bronze.bronze_saneamento (data_importacao DESC);
GO


-- =============================================================================
-- Verificação final
-- =============================================================================
SELECT
    s.name          AS schema_name,
    t.name          AS table_name,
    p.rows          AS total_rows,
    t.create_date   AS created_at
FROM
    sys.tables      t
    JOIN sys.schemas    s ON s.schema_id = t.schema_id
    JOIN sys.partitions p ON p.object_id = t.object_id
                          AND p.index_id IN (0, 1)
WHERE
    s.name = 'bronze'
ORDER BY
    t.name;
GO
