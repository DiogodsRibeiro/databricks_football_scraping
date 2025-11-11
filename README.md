# ⚽ Football Analytics Pipeline - Databricks & Azure

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Delta_Live_Tables-red.svg)](https://www.databricks.com/)
[![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4.svg)](https://azure.microsoft.com/)
[![Selenium](https://img.shields.io/badge/Selenium-Web_Scraping-43B02A.svg)](https://www.selenium.dev/)

Pipeline end-to-end de análise de dados de futebol, desde web scraping até insights avançados usando **Databricks**, **Azure Blob Storage** e **Delta Live Tables**.

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#️-arquitetura)
- [Features](#-features)
- [Tecnologias](#-tecnologias)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Instalação](#-instalação)
- [Pipeline de Dados](#-pipeline-de-dados)
- [Tabelas e Schemas](#-tabelas-e-schemas)
- [Casos de Uso](#-casos-de-uso)
- [Contribuindo](#-contribuindo)
- [Autor](#-autor)

---

## 🎯 Visão Geral

Este projeto implementa um **pipeline completo de dados** para análise de estatísticas de futebol, integrando:

1. **Web Scraping** (Selenium) - Coleta de dados do Flashscore
2. **Azure Blob Storage** - Armazenamento direto em containers
3. **Databricks** - Processamento com Delta Live Tables
4. **Arquitetura Medallion** - Bronze → Silver → Gold

### O que o projeto faz?

- 🔍 **Extrai** dados de 28+ campeonatos (Brasileirão, Champions League, Premier League, etc.)
- ☁️ **Envia diretamente** para Azure Blob Storage (JSON incremental)
- 🧹 **Limpa e padroniza** dados com transformações no Databricks
- 📊 **Gera insights** avançados: xG Analysis, Performance Tracking, Head-to-Head
- 📈 **Disponibiliza** métricas prontas para BI e Machine Learning

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                  LOCAL - WEB SCRAPING (Python)                   │
│                                                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Selenium +  │───▶│   Python     │───▶│    Upload    │     │
│  │ ChromeDriver │    │  Processing  │    │   Direto     │     │
│  └──────────────┘    └──────────────┘    └──────┬───────┘     │
└──────────────────────────────────────────────────┼─────────────┘
                                                    │
                                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AZURE BLOB STORAGE                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Container   │    │  Container   │    │  Container   │     │
│  │  calendar    │    │   results    │    │  statistics  │     │
│  │  (JSON)      │    │   (JSON)     │    │   (JSON)     │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATABRICKS - DELTA LIVE TABLES                      │
│                                                                   │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  🥉 BRONZE LAYER (Auto Loader - Cloud Files)          │     │
│  │  • bronze.calendar      (from Azure Blob)             │     │
│  │  • bronze.results       (from Azure Blob)             │     │
│  │  • bronze.statistics    (from Azure Blob)             │     │
│  └─────────────────────┬─────────────────────────────────┘     │
│                        │                                         │
│                        ▼                                         │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  🥈 SILVER LAYER (Curated & Cleaned)                  │     │
│  │  • silver.fact_calendar                               │     │
│  │  • silver.fact_finished_matches                       │     │
│  │  • silver.fact_statistics                             │     │
│  └─────────────────────┬─────────────────────────────────┘     │
│                        │                                         │
│                        ▼                                         │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  🥇 GOLD LAYER (Analytics & Aggregations)             │     │
│  │  • gold.dim_matches_complete                          │     │
│  │  • gold.fact_team_performance                         │     │
│  │  • gold.fact_xg_analysis                              │     │
│  │  • gold.fact_head_to_head                             │     │
│  │  • gold.fact_attack_defense_metrics                   │     │
│  └─────────────────────┬─────────────────────────────────┘     │
└────────────────────────┼──────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUMPTION LAYER                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Power BI    │    │  Notebooks   │    │   ML Models  │     │
│  │  Dashboards  │    │   Analysis   │    │  Predictions │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### Fluxo de Dados Detalhado

1. **Scraping Local**: Scripts Python com Selenium coletam dados do Flashscore
2. **Upload Direto**: Dados enviados imediatamente para Azure Blob Storage (JSON)
3. **Bronze Layer**: Auto Loader detecta novos arquivos e cria Delta Tables
4. **Silver Layer**: Limpeza, padronização e enriquecimento (data quality checks)
5. **Gold Layer**: Métricas agregadas e analytics-ready (business logic)

> **⚠️ IMPORTANTE**: Não há camada de armazenamento local/raw. O scraping envia **diretamente** para o Azure!

---

## ✨ Features

### Web Scraping
- ✅ **Multi-campeonatos**: 28+ ligas nacionais e internacionais
- ✅ **Coleta Incremental**: Atualiza apenas novos dados (últimos N dias configurável)
- ✅ **Upload Direto**: Envia JSON diretamente para Azure Blob Storage
- ✅ **Retry Automático**: Sistema robusto com 3 tentativas por URL
- ✅ **Merge Inteligente**: Evita duplicatas usando IDs únicos
- ✅ **Logs Detalhados**: Monitoramento completo do processo

### Notebooks de Scraping

#### 1. `get_calendar.py` - Calendário de Partidas
- Coleta jogos **futuros agendados**
- Container Azure: `calendar`
- Output: `all_calendar.json`
- Inclui: data, hora, times, campeonato, temporada, rodada

#### 2. `get_results.py` - Resultados de Partidas
- Coleta resultados dos **últimos N dias** (configurável)
- Container Azure: `results`
- Output: `all_results_incremental.json`
- **Upload incremental**: merge com dados existentes
- Inclui: placares, data, times, rodada

#### 3. `get_statistics_urls.py` - URLs de Estatísticas
- Extrai links de estatísticas dos jogos recentes
- Container Azure: `statistics-urls`
- Output: `statistics_urls.json`
- Filtro: últimos N dias configurável

#### 4. `get_statistics.py` - Estatísticas Detalhadas
- Baixa URLs do Azure e coleta estatísticas completas
- Container Azure: `statistics`
- Output: `incremental_games_statistics.json`
- **Checkpoint a cada 100 jogos**
- Inclui: xG, posse, chutes, passes, cartões, etc.

### Campeonatos Suportados
- 🇧🇷 **Brasil**: Série A, Série B, Copa do Brasil
- 🇦🇷 **Argentina**: Primera División
- 🏆 **América do Sul**: Libertadores, Sul-Americana
- 🏴 **Inglaterra**: Premier League, Championship
- 🇪🇸 **Espanha**: La Liga
- 🇮🇹 **Itália**: Serie A
- 🇩🇪 **Alemanha**: Bundesliga
- 🇫🇷 **França**: Ligue 1
- 🇵🇹 **Portugal**: Liga Portugal
- 🇳🇱 **Holanda**: Eredivisie
- 🏆 **Europa**: Champions League, Europa League
- 🇸🇦 **Arábia Saudita**: Saudi Pro League
- 🇨🇳 **China**: Super Liga Chinesa
- 🇹🇷 **Turquia**: Süper Lig
- E muitos outros...

### Pipeline Databricks
- ✅ **Delta Live Tables**: Pipelines declarativos e auto-gerenciados
- ✅ **Auto Loader (Cloud Files)**: Detecta novos arquivos automaticamente
- ✅ **Streaming**: Processamento incremental
- ✅ **Data Quality**: Validações e constraints em cada camada
- ✅ **Otimização**: Z-Order e particionamento para performance

### Analytics
- ✅ **xG Analysis**: Comparação entre gols esperados (xG) vs reais
- ✅ **Team Performance**: Rankings, pontos, saldo de gols, vitórias
- ✅ **Head-to-Head**: Histórico completo de confrontos diretos
- ✅ **Attack/Defense Metrics**: Eficiência ofensiva e defensiva
- ✅ **Shot Accuracy**: Precisão de finalização por time
- ✅ **Possession Analysis**: Análise de posse de bola

---

## 🛠️ Tecnologias

### Scraping & Upload
- **Python 3.11+**
- **Selenium** - Web scraping automatizado
- **ChromeDriver** - Driver para automação do Chrome
- **Azure SDK (azure-storage-blob)** - Upload direto para Blob Storage
- **python-dotenv** - Gerenciamento de variáveis de ambiente

### Processamento (Databricks)
- **PySpark** - Processamento distribuído
- **Delta Lake** - Storage ACID com versionamento
- **Delta Live Tables (DLT)** - Pipeline orchestration
- **Auto Loader** - Detecção automática de novos arquivos
- **Azure Databricks** - Plataforma lakehouse

### Storage & Infrastructure
- **Azure Blob Storage** - Armazenamento de JSONs
- **Delta Tables** - Tabelas analíticas ACID
- **Unity Catalog** - Governança de dados (opcional)

---

## 📁 Estrutura do Projeto

```
databricks_football_scraping/
│
├── 📓 notebooks/
│   ├── get_calendar.py                  # ✅ Scraping de calendário → Azure
│   ├── get_results.py                   # ✅ Scraping de resultados → Azure
│   ├── get_statistics_urls.py           # ✅ Coleta URLs de estatísticas → Azure
│   └── get_statistics.py                # ✅ Scraping de estatísticas → Azure
│
├── 🔄 dlt_pipelines/
│   ├── bronze/
│   │   ├── ingest_calendar.py           # Auto Loader: Azure → Bronze
│   │   ├── ingest_results.py            # Auto Loader: Azure → Bronze
│   │   └── ingest_statistics.py         # Auto Loader: Azure → Bronze
│   │
│   ├── silver/
│   │   ├── fact_calendar.py             # Calendário limpo
│   │   ├── fact_finished_matches.py     # Resultados limpos
│   │   └── fact_statistics.py           # Estatísticas limpas
│   │
│   └── gold/
│       ├── dim_matches_complete.py      # Visão consolidada
│       ├── fact_team_performance.py     # Agregação por time
│       ├── fact_xg_analysis.py          # Análise xG
│       ├── fact_head_to_head.py         # Confrontos diretos
│       └── fact_attack_defense_metrics.py
│
├── ⚙️ config/
│   └── all_url.json                     # Lista de URLs dos campeonatos
│
├── 📄 .env                               # Credenciais Azure (gitignored)
├── 📄 requirements.txt                   # Dependências Python
├── 📄 .gitignore                        
└── 📄 README.md                          # Este arquivo
```

---

## 🚀 Instalação

### Pré-requisitos

- Python 3.11+
- Google Chrome instalado
- Conta Azure com Blob Storage
- Workspace Databricks (Azure Databricks)
- Git

### 1. Clone o repositório

```bash
git clone https://github.com/DiogodsRibeiro/databricks_football_scraping.git
cd databricks_football_scraping
```

### 2. Configure o ambiente Python

```bash
# Crie um ambiente virtual
python -m venv venv

# Ative o ambiente
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Instale as dependências
pip install -r requirements.txt
```

**requirements.txt:**
```txt
selenium==4.15.0
webdriver-manager==4.0.1
azure-storage-blob==12.19.0
python-dotenv==1.0.0
```

### 3. Configure o ChromeDriver

```bash
# Opção 1: Usando webdriver-manager (recomendado)
pip install webdriver-manager

# O código já usa automaticamente:
from webdriver_manager.chrome import ChromeDriverManager
```

### 4. Configure Azure Blob Storage

Crie um arquivo `.env` na raiz do projeto:

```env
# ⚠️ NÃO COMMITAR ESTE ARQUIVO!
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=seu_storage;AccountKey=sua_chave;EndpointSuffix=core.windows.net"

# Containers (opcionais, já tem defaults no código)
AZURE_CONTAINER_NAME="results"
```

**Como obter a connection string:**
1. Acesse o Portal Azure → Storage Account
2. Vá em "Access Keys"
3. Copie a "Connection String" da Key 1 ou Key 2

**Containers criados automaticamente:**
- `calendar` - Calendário de jogos futuros
- `results` - Resultados de jogos finalizados
- `statistics-urls` - URLs das estatísticas
- `statistics` - Estatísticas detalhadas das partidas

### 5. Configure Databricks

```bash
# Instale Databricks CLI
pip install databricks-cli

# Configure autenticação
databricks configure --token

# Será solicitado:
# Host: https://adb-XXXXXXXXX.XX.azuredatabricks.net
# Token: dapi... (gere em User Settings → Access Tokens)
```

### 6. Monte Azure Blob Storage no Databricks

No Databricks, execute em um notebook:

```python
# Montar Azure Blob Storage
storage_account_name = "seu_storage_account"
storage_account_key = "sua_chave"
container_name = "calendar"  # Repita para cada container

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = f"/mnt/{container_name}",
  extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
  }
)

# Verificar
dbutils.fs.ls(f"/mnt/{container_name}")
```

---

## 🔄 Pipeline de Dados

### Fase 1: Web Scraping → Azure (Local - Python)

#### 1.1 Calendário de Partidas

```python
# notebooks/get_calendar.py

from get_calendar import carga_calendario

# Executa scraping e upload para Azure
carga_calendario()

# Resultado:
# ✅ Arquivo salvo no Azure: all_calendar.json
# Container: calendar
```

**O que faz:**
- Acessa URLs configuradas em `data/json/all_url.json`
- Extrai calendário (jogos futuros)
- Faz upload direto para Azure Blob (container `calendar`)
- Formato: `{"origem": "Brasil", "Campeonato": "Brasileirão A", ...}`

#### 1.2 Resultados de Partidas (Incremental)

```python
# notebooks/get_results.py

from get_results import carga_incremental_results

# Últimos 2 dias (padrão)
carga_incremental_results()

# Ou carregar todos os resultados
carga_incremental_results(carregar_todos=True)

# Resultado:
# ✅ Arquivo salvo no Azure: all_results_incremental.json
# Container: results
```

**Configuração:**
```python
DIAS_RETROATIVOS = 2  # Ajuste conforme necessário
```

**O que faz:**
- Busca resultados dos últimos N dias
- **Merge inteligente**: baixa JSON existente do Azure, evita duplicatas
- Upload incremental (não sobrescreve dados antigos)
- Checkpoint automático

#### 1.3 URLs de Estatísticas

```python
# notebooks/get_statistics_urls.py

from get_statistics_urls import coletar_urls_estatisticas

# Coleta URLs dos últimos 2 dias
coletar_urls_estatisticas()

# Resultado:
# ✅ Arquivo salvo no Azure: statistics_urls.json
# Container: statistics-urls
```

**O que faz:**
- Acessa páginas de resultados
- Filtra jogos dos últimos N dias
- Extrai URLs de estatísticas detalhadas
- Salva lista de URLs no Azure

#### 1.4 Estatísticas Detalhadas

```python
# notebooks/get_statistics.py

from get_statistics import coletar_estatisticas_partidas_incremental

# Baixa URLs do Azure e coleta estatísticas
coletar_estatisticas_partidas_incremental()

# Resultado:
# ✅ Arquivo salvo no Azure: incremental_games_statistics.json
# Container: statistics
# 💾 Checkpoint a cada 100 jogos
```

**Features:**
- **Retry automático**: 3 tentativas por URL
- **Checkpoint**: salva progresso a cada 100 jogos
- **Merge incremental**: evita duplicatas
- **Fallback de seletores**: múltiplos CSS selectors para robustez
- **Log de falhas**: salva URLs com erro em `urls_com_falha.json`

**Configuração:**
```python
DIAS_RETROATIVOS = 2  # Ajuste conforme necessário
```

---

### Fase 2: Azure → Databricks (Bronze Layer)

#### Auto Loader - Detecção Automática de Arquivos

```python
# dlt_pipelines/bronze/ingest_calendar.py

import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="bronze.calendar",
    comment="Raw calendar data from Azure Blob Storage",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_calendar():
    """
    Auto Loader detecta automaticamente novos arquivos em /mnt/calendar
    e carrega incrementalmente para a tabela bronze.calendar
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/bronze/calendar")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/mnt/calendar/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
```

**Principais features:**
- ✅ **Auto Loader**: detecta novos arquivos automaticamente
- ✅ **Schema Evolution**: adiciona novas colunas automaticamente
- ✅ **Checkpoint**: rastreia arquivos já processados
- ✅ **Idempotência**: não processa o mesmo arquivo 2x

#### Ingestão de Resultados

```python
# dlt_pipelines/bronze/ingest_results.py

import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="bronze.results",
    comment="Raw match results from Azure Blob Storage"
)
def bronze_results():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/bronze/results")
        .load("/mnt/results/")
        .withColumn("_ingestion_timestamp", current_timestamp())
    )
```

#### Ingestão de Estatísticas

```python
# dlt_pipelines/bronze/ingest_statistics.py

import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="bronze.statistics",
    comment="Raw match statistics from Azure Blob Storage"
)
def bronze_statistics():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/bronze/statistics")
        .load("/mnt/statistics/")
        .withColumn("_ingestion_timestamp", current_timestamp())
    )
```

---

### Fase 3: Silver Layer - Transformação e Limpeza

#### Calendário Limpo

```python
# dlt_pipelines/silver/fact_calendar.py

import dlt
from pyspark.sql.functions import col, to_timestamp, concat_ws, trim

@dlt.table(
    name="silver.fact_calendar",
    comment="Cleaned and standardized calendar data"
)
@dlt.expect_or_drop("valid_teams", 
    "home_team IS NOT NULL AND away_team IS NOT NULL")
@dlt.expect_or_drop("valid_datetime", 
    "match_datetime IS NOT NULL")
def silver_calendar():
    return (
        dlt.read_stream("bronze.calendar")
        .withColumn("match_datetime", 
            to_timestamp(concat_ws(" ", col("data"), col("hora")), "dd/MM/yyyy HH:mm")
        )
        .select(
            col("id").alias("match_id"),
            col("Campeonato").alias("championship"),
            col("temporada").alias("season"),
            col("rodada").cast("int").alias("round"),
            col("time_casa").alias("home_team"),
            col("time_visitante").alias("away_team"),
            col("origem").alias("country"),
            col("match_datetime")
        )
    )
```

#### Resultados Limpos

```python
# dlt_pipelines/silver/fact_finished_matches.py

import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="silver.fact_finished_matches",
    comment="Cleaned match results"
)
@dlt.expect_or_drop("valid_scores", 
    "placar_casa >= 0 AND placar_visitante >= 0")
def silver_finished_matches():
    return (
        dlt.read_stream("bronze.results")
        .select(
            col("id").alias("match_id"),
            col("campeonato").alias("championship"),
            col("temporada").alias("season"),
            col("rodada").alias("round"),
            col("time_casa").alias("home_team"),
            col("time_visitante").alias("away_team"),
            col("origem").alias("country"),
            col("placar_casa").cast("int").alias("home_score"),
            col("placar_visitante").cast("int").alias("away_score"),
            to_timestamp(concat_ws(" ", col("data"), col("hora")), "dd/MM/yyyy HH:mm").alias("match_date")
        )
    )
```

#### Estatísticas Limpas

```python
# dlt_pipelines/silver/fact_statistics.py

import dlt
from pyspark.sql.functions import col, round as spark_round

@dlt.table(
    name="silver.fact_statistics",
    comment="Cleaned and normalized match statistics"
)
@dlt.expect_or_drop("valid_match_id", "match_id IS NOT NULL")
@dlt.expect("realistic_xg", 
    "expected_goals_xg_home >= 0 AND expected_goals_xg_away >= 0")
@dlt.expect("valid_possession",
    "possession_home + possession_away BETWEEN 95 AND 105")
def silver_statistics():
    return (
        dlt.read_stream("bronze.statistics")
        .select(
            col("id").alias("match_id"),
            col("date").alias("match_date"),
            
            # Expected Goals
            spark_round(col("gols_esperados_xg_home"), 2).alias("expected_goals_xg_home"),
            spark_round(col("gols_esperados_xg_away"), 2).alias("expected_goals_xg_away"),
            
            # Posse
            spark_round(col("posse_de_bola_home"), 1).alias("possession_home"),
            spark_round(col("posse_de_bola_away"), 1).alias("possession_away"),
            
            # Finalizações
            col("total_de_finalizacoes_home").cast("int").alias("total_shots_home"),
            col("total_de_finalizacoes_away").cast("int").alias("total_shots_away"),
            col("finalizacoes_no_alvo_home").cast("int").alias("shots_on_target_home"),
            col("finalizacoes_no_alvo_away").cast("int").alias("shots_on_target_away"),
            
            # Outros...
            col("chances_claras_home").cast("int").alias("big_chances_home"),
            col("chances_claras_away").cast("int").alias("big_chances_away"),
            col("escanteios_home").cast("int").alias("corners_home"),
            col("escanteios_away").cast("int").alias("corners_away")
        )
        .filter(col("match_id").isNotNull())
    )
```

---

### Fase 4: Gold Layer - Analytics

```python
# dlt_pipelines/gold/dim_matches_complete.py

import dlt
from pyspark.sql.functions import col, when, coalesce, round as spark_round

@dlt.table(
    name="gold.dim_matches_complete",
    comment="Complete match view with all metrics for analytics"
)
def gold_matches_complete():
    results = dlt.read("silver.fact_finished_matches")
    stats = dlt.read("silver.fact_statistics")
    
    return (
        results
        .join(stats, results.match_id == stats.match_id, "left")
        .select(
            results.match_id,
            results.match_date.alias("match_datetime"),
            results.championship,
            results.season,
            results.round,
            results.home_team,
            results.away_team,
            results.home_score,
            results.away_score,
            
            # Match Result
            when(results.home_score > results.away_score, "Home Win")
                .when(results.home_score < results.away_score, "Away Win")
                .otherwise("Draw").alias("match_result"),
            
            (results.home_score + results.away_score).alias("total_goals"),
            
            # xG Metrics
            stats.expected_goals_xg_home,
            stats.expected_goals_xg_away,
            spark_round(results.home_score - stats.expected_goals_xg_home, 2).alias("xg_diff_home"),
            spark_round(results.away_score - stats.expected_goals_xg_away, 2).alias("xg_diff_away"),
            
            # Outras estatísticas...
            stats.possession_home,
            stats.possession_away,
            stats.total_shots_home,
            stats.total_shots_away,
            stats.shots_on_target_home,
            stats.shots_on_target_away
        )
    )
```

Para as outras tabelas gold (team_performance, xg_analysis, head_to_head), consulte o código completo fornecido anteriormente.

---

## 📊 Tabelas e Schemas

### Containers Azure Blob Storage

| Container | Descrição | Arquivo | Atualização |
|-----------|-----------|---------|-------------|
| `calendar` | Calendário futuro | `all_calendar.json` | Full refresh |
| `results` | Resultados históricos | `all_results_incremental.json` | Incremental |
| `statistics-urls` | URLs de stats | `statistics_urls.json` | Full refresh |
| `statistics` | Estatísticas detalhadas | `incremental_games_statistics.json` | Incremental |

### Tabelas Bronze (Delta)

| Tabela | Origem | Tipo |
|--------|--------|------|
| `bronze.calendar` | Azure Blob `/mnt/calendar` | Streaming |
| `bronze.results` | Azure Blob `/mnt/results` | Streaming |
| `bronze.statistics` | Azure Blob `/mnt/statistics` | Streaming |

### Tabelas Silver (Delta)

| Tabela | Descrição | Key Columns |
|--------|-----------|-------------|
| `silver.fact_calendar` | Calendário limpo | match_id, match_datetime |
| `silver.fact_finished_matches` | Resultados limpos | match_id, home_score, away_score |
| `silver.fact_statistics` | Estatísticas limpas | match_id, xg_home, xg_away |

### Tabelas Gold (Delta)

| Tabela | Descrição | Use Case |
|--------|-----------|----------|
| `gold.dim_matches_complete` | Visão completa | Análise de jogos |
| `gold.fact_team_performance` | Performance por time | Rankings |
| `gold.fact_xg_analysis` | Análise xG | Over/Under performance |
| `gold.fact_head_to_head` | Confrontos diretos | Rivalidades |
| `gold.fact_attack_defense_metrics` | Métricas de jogo | Eficiência |

---

## 💡 Casos de Uso

### 1. Ranking de Times (Tabela de Classificação)

```sql
SELECT 
    team,
    total_games,
    total_wins,
    total_draws,
    total_losses,
    total_points,
    goal_difference,
    ROUND(total_points / NULLIF(total_games, 0), 2) as points_per_game
FROM gold.fact_team_performance
WHERE championship = 'Brasileirão A'
  AND season = '2024/2025'
ORDER BY total_points DESC, goal_difference DESC
LIMIT 20;
```

### 2. Times que Super Performam (xG Analysis)

```sql
WITH team_xg AS (
    SELECT 
        home_team as team,
        AVG(xg_diff_home) as avg_xg_overperformance,
        COUNT(*) as matches
    FROM gold.fact_xg_analysis
    WHERE season = '2024/2025'
    GROUP BY home_team
    
    UNION ALL
    
    SELECT 
        away_team as team,
        AVG(xg_diff_away) as avg_xg_overperformance,
        COUNT(*) as matches
    FROM gold.fact_xg_analysis
    WHERE season = '2024/2025'
    GROUP BY away_team
)

SELECT 
    team,
    ROUND(AVG(avg_xg_overperformance), 2) as xg_overperformance,
    SUM(matches) as total_matches
FROM team_xg
GROUP BY team
HAVING SUM(matches) >= 10
ORDER BY xg_overperformance DESC
LIMIT 10;
```

### 3. Análise de Confronto Direto

```sql
SELECT 
    home_team,
    away_team,
    COUNT(*) as total_matches,
    SUM(CASE WHEN match_result = 'Home Win' THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN match_result = 'Away Win' THEN 1 ELSE 0 END) as away_wins,
    SUM(CASE WHEN match_result = 'Draw' THEN 1 ELSE 0 END) as draws,
    AVG(total_goals) as avg_goals
FROM gold.dim_matches_complete
WHERE (home_team = 'Flamengo' AND away_team = 'Palmeiras')
   OR (home_team = 'Palmeiras' AND away_team = 'Flamengo')
GROUP BY home_team, away_team;
```

### 4. Dashboard Executivo - KPIs

```sql
SELECT 
    COUNT(DISTINCT match_id) as total_matches,
    SUM(total_goals) as total_goals,
    ROUND(AVG(total_goals), 2) as avg_goals_per_match,
    ROUND(AVG(total_xg), 2) as avg_xg_per_match,
    COUNT(DISTINCT home_team) as total_teams
FROM gold.dim_matches_complete
WHERE season = '2024/2025'
  AND championship = 'Brasileirão A';
```

---

## 📈 Monitoramento

### Data Quality Checks

```python
# Expectations implementadas no DLT

@dlt.expect_or_drop("valid_teams", "home_team IS NOT NULL AND away_team IS NOT NULL")
@dlt.expect_or_drop("valid_score", "home_score >= 0 AND away_score >= 0")
@dlt.expect_or_warn("realistic_possession", "possession_home + possession_away BETWEEN 95 AND 105")
@dlt.expect_or_fail("match_id_unique", "COUNT(DISTINCT match_id) = COUNT(*)")
```

### Logs do Scraping

```python
# Exemplo de saída do get_statistics.py

🔄 Processando URL 1/150: https://...
✅ Coletado: Flamengo_vs_Palmeiras_15012024
💾 Progresso salvo com 100 jogos.
❌ Erro ao processar URL (tentativa 1/3): Timeout
🔄 Reiniciando navegador em 10 segundos...
✅ {len(todos_os_jogos)} jogos coletados com sucesso!
❌ {len(urls_com_falha)} URLs falharam
```

### Métricas do Pipeline DLT

Acesse: **Databricks UI → Delta Live Tables → [Pipeline]**
- Tempo de execução por tabela
- Registros processados
- Data quality violations
- Resource utilization

---

## 🔧 Configuração Avançada

### Ajustar Período de Coleta

```python
# Em get_results.py, get_statistics_urls.py, get_statistics.py
DIAS_RETROATIVOS = 7  # Altere para coletar mais dias
```

### Adicionar Novos Campeonatos

Edite `data/json/all_url.json`:

```json
{
  "urls": [
    "https://www.flashscore.com.br/futebol/brasil/serie-a/{endpoint}/",
    "https://www.flashscore.com.br/futebol/europa/champions-league/{endpoint}/",
    "SUA_NOVA_URL/{endpoint}/"
  ]
}
```

### Otimização de Performance

```sql
-- Z-Ordering
OPTIMIZE gold.dim_matches_complete
ZORDER BY (championship, season, match_datetime, home_team);

-- Vacuum (remover arquivos antigos)
VACUUM gold.dim_matches_complete RETAIN 168 HOURS;
```

### Scheduling Automático

```bash
# Databricks Jobs - Agendar scraping + pipeline
# 1. Crie um Job no Databricks UI
# 2. Adicione tasks:
#    - Task 1: Executar notebooks Python (scraping)
#    - Task 2: Trigger DLT Pipeline
# 3. Schedule: Cron "0 6 * * *" (6h da manhã diariamente)
```

---

</div>
