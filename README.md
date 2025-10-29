# ⚽ Football Analytics Pipeline - Databricks & Azure

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Delta_Live_Tables-red.svg)](https://www.databricks.com/)
[![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4.svg)](https://azure.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Pipeline end-to-end de análise de dados de futebol, desde web scraping até insights avançados usando **Databricks**, **Azure** e **Delta Live Tables**.

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
2. **Azure Storage** - Armazenamento em Data Lake
3. **Databricks** - Processamento com Delta Live Tables
4. **Arquitetura Medallion** - Bronze → Silver → Gold

### O que o projeto faz?

- 🔍 **Extrai** dados de 28+ campeonatos (Brasileirão, Champions League, Premier League, etc.)
- 🧹 **Limpa e padroniza** dados com transformações no Databricks
- 📊 **Gera insights** avançados: xG Analysis, Performance Tracking, Head-to-Head
- 📈 **Disponibiliza** métricas prontas para BI e Machine Learning

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                     LOCAL - WEB SCRAPING                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Selenium    │───▶│   Pandas     │───▶│  Parquet/    │     │
│  │  ChromeDriver│    │  Processing  │    │  JSON Files  │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      AZURE DATA LAKE GEN2                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │   Landing    │    │    Raw       │    │   Staging    │     │
│  │   /calendar  │    │  /results    │    │ /statistics  │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATABRICKS - DELTA LIVE TABLES                      │
│                                                                   │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  🥉 BRONZE LAYER (Raw Delta Tables)                   │     │
│  │  • bronze.calendar                                     │     │
│  │  • bronze.results                                      │     │
│  │  • bronze.statistics                                   │     │
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

### Fluxo de Dados

1. **Coleta (Local)**: Notebook Jupyter + Selenium → Scraping do Flashscore
2. **Ingestão (Azure)**: Upload para Azure Data Lake Storage
3. **Bronze Layer**: Dados brutos em Delta Tables (schema-on-read)
4. **Silver Layer**: Limpeza, padronização e enriquecimento (data quality checks)
5. **Gold Layer**: Métricas agregadas e analytics-ready (business logic)

---

## ✨ Features

### Web Scraping
- ✅ **Multi-campeonatos**: 28+ ligas nacionais e internacionais
- ✅ **Coleta Incremental**: Atualiza apenas novos dados (últimos N dias)
- ✅ **Retry Automático**: Sistema robusto com tratamento de erros
- ✅ **Logs Detalhados**: Monitoramento completo do processo
- ✅ **Selenium WebDriver**: Automação de navegação no Flashscore

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
- ✅ **Streaming**: Processamento incremental com Auto Loader
- ✅ **Data Quality**: Validações e constraints em cada camada
- ✅ **Otimização**: Z-Order e particionamento para performance
- ✅ **Unity Catalog**: Governança e descoberta de dados

### Analytics
- ✅ **xG Analysis**: Comparação entre gols esperados (xG) vs reais
- ✅ **Team Performance**: Rankings, pontos, saldo de gols, vitórias
- ✅ **Head-to-Head**: Histórico completo de confrontos diretos
- ✅ **Attack/Defense Metrics**: Eficiência ofensiva e defensiva
- ✅ **Shot Accuracy**: Precisão de finalização por time
- ✅ **Possession Analysis**: Análise de posse de bola

---

## 🛠️ Tecnologias

### Scraping & Ingestão
- **Python 3.11+**
- **Selenium** - Web scraping automatizado
- **ChromeDriver** - Driver para automação do Chrome
- **Pandas** - Manipulação de dados
- **Azure SDK** - Upload para Data Lake
- **Requests** - HTTP requests

### Processamento (Databricks)
- **PySpark** - Processamento distribuído
- **Delta Lake** - Storage ACID com versionamento e time travel
- **Delta Live Tables (DLT)** - Pipeline orchestration
- **Azure Databricks** - Plataforma lakehouse
- **SQL Analytics** - Queries e análises

### Storage & Infrastructure
- **Azure Data Lake Gen2** - Raw data storage
- **Delta Tables** - Tabelas analíticas ACID
- **Unity Catalog** - Governança de dados
- **Azure Blob Storage** - Armazenamento complementar

### Development Tools
- **Jupyter Notebooks** - Desenvolvimento e testes
- **Git** - Versionamento de código
- **Databricks CLI** - Automação de deploys
- **VS Code** - Editor de código

---

## 📁 Estrutura do Projeto

```
databricks_football_scraping/
│
├── 📓 notebooks/
│   ├── 01_web_scraping_calendar.ipynb      # Scraping de calendário
│   ├── 02_web_scraping_results.ipynb       # Scraping de resultados
│   ├── 03_web_scraping_statistics.ipynb    # Scraping de estatísticas
│   ├── 04_upload_to_azure.ipynb            # Upload para Data Lake
│   └── 05_data_quality_checks.ipynb        # Validações de dados
│
├── 🔄 dlt_pipelines/
│   ├── bronze/
│   │   ├── ingest_calendar.py              # Ingestão calendário (Auto Loader)
│   │   ├── ingest_results.py               # Ingestão resultados
│   │   └── ingest_statistics.py            # Ingestão estatísticas
│   │
│   ├── silver/
│   │   ├── fact_calendar.py                # Calendário limpo e padronizado
│   │   ├── fact_finished_matches.py        # Resultados limpos
│   │   └── fact_statistics.py              # Estatísticas limpas e normalizadas
│   │
│   └── gold/
│       ├── dim_matches_complete.py         # Visão consolidada de partidas
│       ├── fact_team_performance.py        # Agregação por time
│       ├── fact_xg_analysis.py             # Análise xG vs Real
│       ├── fact_head_to_head.py            # Confrontos diretos
│       └── fact_attack_defense_metrics.py  # Métricas de jogo detalhadas
│
├── ⚙️ config/
│   ├── championships.json                  # Lista de campeonatos
│   ├── azure_config.yaml                   # Credenciais Azure (gitignored)
│   └── scraping_config.json                # Configurações de scraping
│
├── 🛠️ utils/
│   ├── scraping_utils.py                   # Funções auxiliares de scraping
│   ├── azure_utils.py                      # Funções de upload Azure
│   ├── data_validation.py                  # Validações de dados
│   └── logger.py                           # Sistema de logging
│
├── 🧪 tests/
│   ├── test_scraping.py                    # Testes de scraping
│   └── test_transformations.py             # Testes de transformações
│
├── 📊 data/                                 # Dados locais (gitignored)
│   ├── raw/                                # Dados brutos do scraping
│   ├── staging/                            # Dados intermediários
│   └── processed/                          # Dados processados
│
├── 📝 logs/                                 # Logs de execução (gitignored)
│
├── 📄 requirements.txt                      # Dependências Python
├── 📄 .gitignore                           
├── 📄 LICENSE
└── 📄 README.md                            # Este arquivo
```

---

## 🚀 Instalação

### Pré-requisitos

- Python 3.11+
- Google Chrome instalado
- Conta Azure com Data Lake Gen2
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

### 3. Baixe o ChromeDriver

```bash
# Opção 1: Manual
# Baixe de https://chromedriver.chromium.org/
# Adicione ao PATH do sistema

# Opção 2: Automático com webdriver-manager
pip install webdriver-manager
```

### 4. Configure Azure

Crie um arquivo `config/azure_config.yaml`:

```yaml
# NÃO COMMITAR ESTE ARQUIVO!
storage_account_name: "seu_storage_account"
container_name: "football-data"
sas_token: "seu_sas_token_aqui"

# Ou usando connection string
connection_string: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."

# Estrutura de pastas no Data Lake
landing_path: "landing/raw"
bronze_path: "bronze"
silver_path: "silver"
gold_path: "gold"
```

**⚠️ IMPORTANTE**: Adicione este arquivo ao `.gitignore` para não expor credenciais!

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

### 6. Teste a instalação

```bash
# Teste o scraping
python -m utils.scraping_utils --test

# Teste conexão Azure
python -m utils.azure_utils --test-connection

# Teste Databricks CLI
databricks workspace ls /
```

---

## 🔄 Pipeline de Dados

### Fase 1: Web Scraping (Local - Jupyter Notebooks)

#### Calendário de Partidas

```python
# notebooks/01_web_scraping_calendar.ipynb

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from utils.scraping_utils import setup_driver, scrape_calendar_data
import pandas as pd

# Configurar driver
driver = setup_driver(headless=True)

# Lista de campeonatos
championships = [
    'brasileirao-a',
    'premier-league',
    'la-liga',
    'serie-a',
    'bundesliga',
    'champions-league'
]

# Coletar calendário
calendar_data = []
for championship in championships:
    print(f"Scraping {championship}...")
    data = scrape_calendar_data(driver, championship)
    calendar_data.extend(data)

# Converter para DataFrame
df_calendar = pd.DataFrame(calendar_data)

# Salvar localmente
df_calendar.to_parquet('data/raw/calendar.parquet', index=False)
print(f"✅ Coletados {len(df_calendar)} jogos agendados")

driver.quit()
```

#### Resultados de Partidas

```python
# notebooks/02_web_scraping_results.ipynb

from utils.scraping_utils import scrape_results_incremental
import pandas as pd
from datetime import datetime, timedelta

# Coletar resultados dos últimos 7 dias
days_back = 7
end_date = datetime.now()
start_date = end_date - timedelta(days=days_back)

print(f"Coletando resultados de {start_date.date()} até {end_date.date()}")

results_data = scrape_results_incremental(
    driver,
    start_date=start_date,
    end_date=end_date,
    championships=championships
)

df_results = pd.DataFrame(results_data)
df_results.to_parquet('data/raw/results.parquet', index=False)

print(f"✅ Coletados {len(df_results)} resultados")
```

#### Estatísticas Detalhadas

```python
# notebooks/03_web_scraping_statistics.ipynb

from utils.scraping_utils import scrape_match_statistics

# Obter IDs das partidas dos resultados
match_ids = df_results['id'].unique().tolist()

print(f"Coletando estatísticas de {len(match_ids)} partidas...")

statistics_data = []
for i, match_id in enumerate(match_ids, 1):
    print(f"[{i}/{len(match_ids)}] Match ID: {match_id}")
    
    try:
        stats = scrape_match_statistics(driver, match_id)
        if stats:
            statistics_data.append(stats)
    except Exception as e:
        print(f"❌ Erro no match {match_id}: {e}")
        continue

df_statistics = pd.DataFrame(statistics_data)
df_statistics.to_parquet('data/raw/statistics.parquet', index=False)

print(f"✅ Coletadas estatísticas de {len(df_statistics)} partidas")
```

### Fase 2: Upload para Azure Data Lake

```python
# notebooks/04_upload_to_azure.ipynb

from azure.storage.blob import BlobServiceClient
from utils.azure_utils import upload_file_to_blob, load_azure_config
import os

# Carregar configurações
config = load_azure_config('config/azure_config.yaml')

# Conectar ao Azure
blob_service_client = BlobServiceClient.from_connection_string(
    config['connection_string']
)

container_client = blob_service_client.get_container_client(
    config['container_name']
)

# Upload dos arquivos
files_to_upload = {
    'data/raw/calendar.parquet': 'landing/raw/calendar/calendar_latest.parquet',
    'data/raw/results.parquet': 'landing/raw/results/results_latest.parquet',
    'data/raw/statistics.parquet': 'landing/raw/statistics/statistics_latest.parquet'
}

for local_path, blob_path in files_to_upload.items():
    print(f"Uploading {local_path} → {blob_path}")
    
    with open(local_path, 'rb') as data:
        container_client.upload_blob(
            name=blob_path,
            data=data,
            overwrite=True
        )
    
    print(f"✅ Upload concluído: {blob_path}")

print("\n✅ Todos os arquivos foram enviados para o Azure Data Lake!")
```

### Fase 3: Pipeline Delta Live Tables (Databricks)

#### Bronze Layer - Ingestão com Auto Loader

```python
# dlt_pipelines/bronze/ingest_calendar.py

import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="bronze.calendar",
    comment="Raw calendar data ingested from Azure Data Lake",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_calendar():
    """
    Ingestão incremental de dados de calendário usando Auto Loader.
    Auto Loader detecta automaticamente novos arquivos no Data Lake.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", 
                "/mnt/schemas/bronze/calendar")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/mnt/datalake/landing/raw/calendar/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
```

```python
# dlt_pipelines/bronze/ingest_statistics.py

import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="bronze.statistics",
    comment="Raw match statistics from Azure Data Lake",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_statistics():
    """
    Ingestão de estatísticas detalhadas das partidas.
    Inclui métricas como xG, posse de bola, chutes, etc.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", 
                "/mnt/schemas/bronze/statistics")
        .load("/mnt/datalake/landing/raw/statistics/")
        .withColumn("_ingestion_timestamp", current_timestamp())
    )
```

#### Silver Layer - Transformação e Limpeza

```python
# dlt_pipelines/silver/fact_calendar.py

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, trim, concat_ws, 
    regexp_replace, upper, when
)

@dlt.table(
    name="silver.fact_calendar",
    comment="Cleaned and standardized calendar data with data quality checks"
)
@dlt.expect_or_drop("valid_teams", 
    "home_team IS NOT NULL AND away_team IS NOT NULL")
@dlt.expect_or_drop("valid_datetime", 
    "match_datetime IS NOT NULL")
@dlt.expect_or_warn("future_matches", 
    "match_datetime > current_timestamp()")
def silver_calendar():
    """
    Transformações aplicadas:
    - Padronização de nomes de times
    - Conversão de data/hora para timestamp
    - Normalização de campeonatos
    - Remoção de caracteres especiais
    """
    return (
        dlt.read_stream("bronze.calendar")
        
        # Conversão de data/hora
        .withColumn("match_datetime", 
            to_timestamp(
                concat_ws(" ", col("data"), col("hora")), 
                "dd/MM/yyyy HH:mm"
            )
        )
        
        # Limpeza de nomes
        .withColumn("home_team_clean", 
            trim(regexp_replace(col("time_casa"), "[^a-zA-Z0-9 ]", ""))
        )
        .withColumn("away_team_clean",
            trim(regexp_replace(col("time_visitante"), "[^a-zA-Z0-9 ]", ""))
        )
        
        # Seleção e renomeação de colunas
        .select(
            col("id").alias("match_id"),
            col("Campeonato").alias("championship"),
            col("temporada").alias("season"),
            col("rodada").cast("int").alias("round"),
            col("home_team_clean").alias("home_team"),
            col("away_team_clean").alias("away_team"),
            col("origem").alias("country"),
            col("match_datetime"),
            col("_ingestion_timestamp")
        )
    )
```

```python
# dlt_pipelines/silver/fact_statistics.py

import dlt
from pyspark.sql.functions import col, round as spark_round

@dlt.table(
    name="silver.fact_statistics",
    comment="Match statistics with standardized metrics and English column names"
)
@dlt.expect_or_drop("valid_match_id", "match_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "match_date IS NOT NULL")
@dlt.expect("realistic_xg", 
    "expected_goals_xg_home >= 0 AND expected_goals_xg_away >= 0")
@dlt.expect("valid_possession",
    "possession_home + possession_away BETWEEN 95 AND 105")
def silver_statistics():
    """
    Normalização de estatísticas com:
    - Tradução de nomes de colunas para inglês
    - Validações de valores realistas
    - Arredondamento de métricas
    """
    return (
        dlt.read_stream("bronze.statistics")
        
        .select(
            # Identificadores
            col("id").alias("match_id"),
            col("date").alias("match_date"),
            
            # Expected Goals & Assists
            spark_round(col("gols_esperados_xg_home"), 2).alias("expected_goals_xg_home"),
            spark_round(col("gols_esperados_xg_away"), 2).alias("expected_goals_xg_away"),
            spark_round(col("assistencias_esperadas_xa_home"), 2).alias("expected_assists_xa_home"),
            spark_round(col("assistencias_esperadas_xa_away"), 2).alias("expected_assists_xa_away"),
            
            # Posse de Bola
            spark_round(col("posse_de_bola_home"), 1).alias("possession_home"),
            spark_round(col("posse_de_bola_away"), 1).alias("possession_away"),
            
            # Finalizações
            col("total_de_finalizacoes_home").cast("int").alias("total_shots_home"),
            col("total_de_finalizacoes_away").cast("int").alias("total_shots_away"),
            col("finalizacoes_no_alvo_home").cast("int").alias("shots_on_target_home"),
            col("finalizacoes_no_alvo_away").cast("int").alias("shots_on_target_away"),
            col("finalizacoes_bloqueadas_home").cast("int").alias("blocked_shots_home"),
            col("finalizacoes_bloqueadas_away").cast("int").alias("blocked_shots_away"),
            
            # Chances Criadas
            col("chances_claras_home").cast("int").alias("big_chances_home"),
            col("chances_claras_away").cast("int").alias("big_chances_away"),
            
            # Defesa
            col("defesas_do_goleiro_home").cast("int").alias("goalkeeper_saves_home"),
            col("defesas_do_goleiro_away").cast("int").alias("goalkeeper_saves_away"),
            col("desarmes_home").cast("int").alias("tackles_home"),
            col("desarmes_away").cast("int").alias("tackles_away"),
            col("interceptacoes_home").cast("int").alias("interceptions_home"),
            col("interceptacoes_away").cast("int").alias("interceptions_away"),
            
            # Disciplina
            col("cartoes_amarelos_home").cast("int").alias("yellow_cards_home"),
            col("cartoes_amarelos_away").cast("int").alias("yellow_cards_away"),
            col("faltas_home").cast("int").alias("fouls_committed_home"),
            col("faltas_away").cast("int").alias("fouls_committed_away"),
            
            # Outros
            col("escanteios_home").cast("int").alias("corners_home"),
            col("escanteios_away").cast("int").alias("corners_away"),
            col("impedimentos_home").cast("int").alias("offsides_home"),
            col("impedimentos_away").cast("int").alias("offsides_away"),
            col("passes_home").cast("int").alias("total_passes_home"),
            col("passes_away").cast("int").alias("total_passes_away")
        )
        
        .filter(col("match_id").isNotNull())
        .filter(col("match_date").isNotNull())
    )
```

#### Gold Layer - Analytics e Agregações

```python
# dlt_pipelines/gold/dim_matches_complete.py

import dlt
from pyspark.sql.functions import (
    col, coalesce, when, lit, 
    round as spark_round
)

@dlt.table(
    name="gold.dim_matches_complete",
    comment="Complete match dimension with results and statistics for analytics",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_matches_complete():
    """
    Tabela dimensional principal unindo:
    - Resultados finais
    - Estatísticas detalhadas
    - Informações de calendário
    
    Esta é a tabela principal para análise de partidas.
    """
    results = dlt.read("silver.fact_finished_matches")
    stats = dlt.read("silver.fact_statistics")
    calendar = dlt.read("silver.fact_calendar")
    
    return (
        results
        .join(stats, results.match_id == stats.match_id, "left")
        .join(calendar, results.match_id == calendar.match_id, "left")
        .select(
            # Identificadores
            results.match_id,
            coalesce(results.match_date, calendar.match_datetime).alias("match_datetime"),
            results.championship,
            results.season,
            results.round,
            results.country,
            
            # Times
            results.home_team,
            results.away_team,
            
            # Resultado
            results.home_score,
            results.away_score,
            when(results.home_score > results.away_score, "Home Win")
                .when(results.home_score < results.away_score, "Away Win")
                .otherwise("Draw").alias("match_result"),
            (results.home_score + results.away_score).alias("total_goals"),
            
            # Expected Goals (xG)
            stats.expected_goals_xg_home,
            stats.expected_goals_xg_away,
            spark_round(
                (stats.expected_goals_xg_home + stats.expected_goals_xg_away), 2
            ).alias("total_xg"),
            
            # Diferença xG vs Real
            spark_round(
                results.home_score - stats.expected_goals_xg_home, 2
            ).alias("xg_diff_home"),
            spark_round(
                results.away_score - stats.expected_goals_xg_away, 2
            ).alias("xg_diff_away"),
            
            # Posse de Bola
            stats.possession_home,
            stats.possession_away,
            
            # Finalizações
            stats.total_shots_home,
            stats.total_shots_away,
            stats.shots_on_target_home,
            stats.shots_on_target_away,
            
            # Eficiência de Finalização (%)
            when(stats.total_shots_home > 0, 
                 spark_round((stats.shots_on_target_home / stats.total_shots_home) * 100, 2))
                .otherwise(0).alias("shot_accuracy_home_pct"),
            when(stats.total_shots_away > 0,
                 spark_round((stats.shots_on_target_away / stats.total_shots_away) * 100, 2))
                .otherwise(0).alias("shot_accuracy_away_pct"),
            
            # Chances e Criatividade
            stats.big_chances_home,
            stats.big_chances_away,
            stats.expected_assists_xa_home,
            stats.expected_assists_xa_away,
            
            # Defesa
            stats.goalkeeper_saves_home,
            stats.goalkeeper_saves_away,
            stats.tackles_home,
            stats.tackles_away,
            stats.interceptions_home,
            stats.interceptions_away,
            
            # Disciplina
            stats.yellow_cards_home,
            stats.yellow_cards_away,
            stats.fouls_committed_home,
            stats.fouls_committed_away,
            
            # Set Pieces
            stats.corners_home,
            stats.corners_away,
            
            # Outros
            stats.offsides_home,
            stats.offsides_away,
            stats.total_passes_home,
            stats.total_passes_away
        )
    )

# Otimização da tabela gold
@dlt.table(
    name="gold.dim_matches_complete_optimized"
)
def optimize_matches():
    """
    Aplica Z-Ordering para melhorar performance de queries.
    Z-Order nas colunas mais usadas em filtros e joins.
    """
    return (
        spark.sql("""
            OPTIMIZE gold.dim_matches_complete
            ZORDER BY (championship, season, match_datetime, home_team, away_team)
        """)
    )
```

```python
# dlt_pipelines/gold/fact_team_performance.py

import dlt
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    round as spark_round, when, lit, coalesce
)

@dlt.table(
    name="gold.fact_team_performance",
    comment="Aggregated team performance metrics by championship and season",
    table_properties={
        "quality": "gold"
    }
)
def gold_team_performance():
    """
    Métricas agregadas por time incluindo:
    - Jogos, vitórias, empates, derrotas
    - Pontos totais
    - Gols marcados e sofridos
    - Médias de xG, posse, finalizações
    - Performance em casa vs fora
    """
    matches = dlt.read("gold.dim_matches_complete")
    
    # Performance em casa
    home_stats = (
        matches
        .groupBy("home_team", "championship", "season", "country")
        .agg(
            count("*").alias("games_played_home"),
            spark_sum(when(col("match_result") == "Home Win", 1).otherwise(0)).alias("wins_home"),
            spark_sum(when(col("match_result") == "Draw", 1).otherwise(0)).alias("draws_home"),
            spark_sum(when(col("match_result") == "Away Win", 1).otherwise(0)).alias("losses_home"),
            spark_sum("home_score").alias("goals_scored_home"),
            spark_sum("away_score").alias("goals_conceded_home"),
            spark_round(avg("possession_home"), 2).alias("avg_possession_home"),
            spark_round(avg("expected_goals_xg_home"), 2).alias("avg_xg_home"),
            spark_round(avg("shots_on_target_home"), 2).alias("avg_shots_on_target_home"),
            spark_round(avg("shot_accuracy_home_pct"), 2).alias("avg_shot_accuracy_home")
        )
        .withColumnRenamed("home_team", "team")
    )
    
    # Performance fora
    away_stats = (
        matches
        .groupBy("away_team", "championship", "season", "country")
        .agg(
            count("*").alias("games_played_away"),
            spark_sum(when(col("match_result") == "Away Win", 1).otherwise(0)).alias("wins_away"),
            spark_sum(when(col("match_result") == "Draw", 1).otherwise(0)).alias("draws_away"),
            spark_sum(when(col("match_result") == "Home Win", 1).otherwise(0)).alias("losses_away"),
            spark_sum("away_score").alias("goals_scored_away"),
            spark_sum("home_score").alias("goals_conceded_away"),
            spark_round(avg("possession_away"), 2).alias("avg_possession_away"),
            spark_round(avg("expected_goals_xg_away"), 2).alias("avg_xg_away"),
            spark_round(avg("shots_on_target_away"), 2).alias("avg_shots_on_target_away"),
            spark_round(avg("shot_accuracy_away_pct"), 2).alias("avg_shot_accuracy_away")
        )
        .withColumnRenamed("away_team", "team")
    )
    
    # Combinar estatísticas
    return (
        home_stats
        .join(away_stats, ["team", "championship", "season", "country"], "outer")
        .select(
            col("team"),
            col("championship"),
            col("season"),
            col("country"),
            
            # Jogos Totais
            (coalesce("games_played_home", lit(0)) + coalesce("games_played_away", lit(0))).alias("total_games"),
            coalesce("games_played_home", lit(0)).alias("games_home"),
            coalesce("games_played_away", lit(0)).alias("games_away"),
            
            # Vitórias/Empates/Derrotas
            (coalesce("wins_home", lit(0)) + coalesce("wins_away", lit(0))).alias("total_wins"),
            (coalesce("draws_home", lit(0)) + coalesce("draws_away", lit(0))).alias("total_draws"),
            (coalesce("losses_home", lit(0)) + coalesce("losses_away", lit(0))).alias("total_losses"),
            
            # Pontos (3 por vitória, 1 por empate)
            ((coalesce("wins_home", lit(0)) + coalesce("wins_away", lit(0))) * 3 + 
             (coalesce("draws_home", lit(0)) + coalesce("draws_away", lit(0)))).alias("total_points"),
            
            # Gols
            (coalesce("goals_scored_home", lit(0)) + coalesce("goals_scored_away", lit(0))).alias("total_goals_scored"),
            (coalesce("goals_conceded_home", lit(0)) + coalesce("goals_conceded_away", lit(0))).alias("total_goals_conceded"),
            (coalesce("goals_scored_home", lit(0)) + coalesce("goals_scored_away", lit(0)) - 
             coalesce("goals_conceded_home", lit(0)) - coalesce("goals_conceded_away", lit(0))).alias("goal_difference"),
            
            # Médias Gerais
            spark_round(
                (coalesce("avg_possession_home", lit(0)) + coalesce("avg_possession_away", lit(0))) / 2, 
                2
            ).alias("avg_possession"),
            spark_round(
                (coalesce("avg_xg_home", lit(0)) + coalesce("avg_xg_away", lit(0))) / 2,
                2
            ).alias("avg_xg"),
            spark_round(
                (coalesce("avg_shots_on_target_home", lit(0)) + coalesce("avg_shots_on_target_away", lit(0))) / 2,
                2
            ).alias("avg_shots_on_target"),
            spark_round(
                (coalesce("avg_shot_accuracy_home", lit(0)) + coalesce("avg_shot_accuracy_away", lit(0))) / 2,
                2
            ).alias("avg_shot_accuracy_pct"),
            
            # Performance em Casa
            coalesce("wins_home", lit(0)).alias("wins_home"),
            coalesce("goals_scored_home", lit(0)).alias("goals_scored_home"),
            coalesce("avg_xg_home", lit(0)).alias("avg_xg_home"),
            
            # Performance Fora
            coalesce("wins_away", lit(0)).alias("wins_away"),
            coalesce("goals_scored_away", lit(0)).alias("goals_scored_away"),
            coalesce("avg_xg_away", lit(0)).alias("avg_xg_away")
        )
        .orderBy(col("total_points").desc(), col("goal_difference").desc())
    )
```

```python
# dlt_pipelines/gold/fact_xg_analysis.py

import dlt
from pyspark.sql.functions import col, round as spark_round, when

@dlt.table(
    name="gold.fact_xg_analysis",
    comment="xG performance analysis - comparing expected vs actual goals"
)
def gold_xg_analysis():
    """
    Análise de Expected Goals (xG):
    - Identifica times que convertem acima/abaixo do esperado
    - Eficiência de finalização
    - Over/Under performance
    """
    matches = dlt.read("gold.dim_matches_complete")
    
    return (
        matches
        .select(
            col("match_id"),
            col("match_datetime"),
            col("championship"),
            col("season"),
            col("home_team"),
            col("away_team"),
            
            # Gols Reais
            col("home_score"),
            col("away_score"),
            
            # Expected Goals
            col("expected_goals_xg_home"),
            col("expected_goals_xg_away"),
            
            # Diferença: Real vs Esperado
            col("xg_diff_home"),
            col("xg_diff_away"),
            
            # Eficiência de conversão (%)
            when(col("expected_goals_xg_home") > 0,
                 spark_round((col("home_score") / col("expected_goals_xg_home")) * 100, 2))
                .otherwise(None).alias("conversion_efficiency_home_pct"),
            when(col("expected_goals_xg_away") > 0,
                 spark_round((col("away_score") / col("expected_goals_xg_away")) * 100, 2))
                .otherwise(None).alias("conversion_efficiency_away_pct"),
            
            # Classificação da performance
            when(col("xg_diff_home") > 1.5, "Home Overperformed")
                .when(col("xg_diff_away") > 1.5, "Away Overperformed")
                .when(col("xg_diff_home") < -1.5, "Home Underperformed")
                .when(col("xg_diff_away") < -1.5, "Away Underperformed")
                .otherwise("As Expected").alias("performance_classification"),
            
            # Flags booleanas para análise
            (col("xg_diff_home") > 0).alias("home_exceeded_xg"),
            (col("xg_diff_away") > 0).alias("away_exceeded_xg")
        )
        .filter(col("expected_goals_xg_home").isNotNull())
        .filter(col("expected_goals_xg_away").isNotNull())
    )
```

---

## 📊 Tabelas e Schemas

### Sumário de Tabelas

| Layer | Tabela | Tipo | Descrição |
|-------|--------|------|-----------|
| 🥉 Bronze | `bronze.calendar` | Streaming | Calendário raw do Data Lake |
| 🥉 Bronze | `bronze.results` | Streaming | Resultados raw |
| 🥉 Bronze | `bronze.statistics` | Streaming | Estatísticas raw |
| 🥈 Silver | `silver.fact_calendar` | Streaming | Calendário limpo |
| 🥈 Silver | `silver.fact_finished_matches` | Streaming | Resultados limpos |
| 🥈 Silver | `silver.fact_statistics` | Streaming | Estatísticas padronizadas |
| 🥇 Gold | `gold.dim_matches_complete` | Batch | Visão consolidada de partidas |
| 🥇 Gold | `gold.fact_team_performance` | Batch | Agregações por time |
| 🥇 Gold | `gold.fact_xg_analysis` | Batch | Análise de xG |
| 🥇 Gold | `gold.fact_head_to_head` | Batch | Confrontos diretos |
| 🥇 Gold | `gold.fact_attack_defense_metrics` | Batch | Métricas de jogo |

### Schema Detalhado - Gold Layer

#### `gold.dim_matches_complete`

```python
match_id: string                      # ID único da partida
match_datetime: timestamp             # Data/hora da partida
championship: string                  # Nome do campeonato
season: string                        # Temporada (ex: "2024/2025")
round: int                           # Número da rodada
country: string                      # País do campeonato
home_team: string                    # Time da casa
away_team: string                    # Time visitante
home_score: int                      # Gols do time da casa
away_score: int                      # Gols do visitante
match_result: string                 # "Home Win", "Away Win", ou "Draw"
total_goals: int                     # Total de gols na partida
expected_goals_xg_home: double       # xG do time da casa
expected_goals_xg_away: double       # xG do visitante
total_xg: double                     # xG total da partida
xg_diff_home: double                 # Diferença real vs esperado (casa)
xg_diff_away: double                 # Diferença real vs esperado (fora)
possession_home: double              # Posse de bola % (casa)
possession_away: double              # Posse de bola % (fora)
total_shots_home: int                # Total de chutes (casa)
total_shots_away: int                # Total de chutes (fora)
shots_on_target_home: int            # Chutes no alvo (casa)
shots_on_target_away: int            # Chutes no alvo (fora)
shot_accuracy_home_pct: double       # Precisão de chutes % (casa)
shot_accuracy_away_pct: double       # Precisão de chutes % (fora)
big_chances_home: int                # Grandes chances (casa)
big_chances_away: int                # Grandes chances (fora)
expected_assists_xa_home: double     # xA (casa)
expected_assists_xa_away: double     # xA (fora)
goalkeeper_saves_home: int           # Defesas do goleiro (casa)
goalkeeper_saves_away: int           # Defesas do goleiro (fora)
tackles_home: int                    # Desarmes (casa)
tackles_away: int                    # Desarmes (fora)
interceptions_home: int              # Interceptações (casa)
interceptions_away: int              # Interceptações (fora)
yellow_cards_home: int               # Cartões amarelos (casa)
yellow_cards_away: int               # Cartões amarelos (fora)
fouls_committed_home: int            # Faltas cometidas (casa)
fouls_committed_away: int            # Faltas cometidas (fora)
corners_home: int                    # Escanteios (casa)
corners_away: int                    # Escanteios (fora)
offsides_home: int                   # Impedimentos (casa)
offsides_away: int                   # Impedimentos (fora)
total_passes_home: int               # Total de passes (casa)
total_passes_away: int               # Total de passes (fora)
```

#### `gold.fact_team_performance`

```python
team: string                         # Nome do time
championship: string                 # Campeonato
season: string                       # Temporada
country: string                      # País
total_games: int                     # Total de jogos
games_home: int                      # Jogos em casa
games_away: int                      # Jogos fora
total_wins: int                      # Total de vitórias
total_draws: int                     # Total de empates
total_losses: int                    # Total de derrotas
total_points: int                    # Pontos totais (3 por vitória)
total_goals_scored: int              # Gols marcados
total_goals_conceded: int            # Gols sofridos
goal_difference: int                 # Saldo de gols
avg_possession: double               # Posse média %
avg_xg: double                       # xG médio por jogo
avg_shots_on_target: double          # Chutes no alvo médios
avg_shot_accuracy_pct: double        # Precisão média de chutes %
wins_home: int                       # Vitórias em casa
goals_scored_home: int               # Gols em casa
avg_xg_home: double                  # xG médio em casa
wins_away: int                       # Vitórias fora
goals_scored_away: int               # Gols fora
avg_xg_away: double                  # xG médio fora
```

---

## 💡 Casos de Uso

### 1. Ranking de Times (Tabela de Classificação)

```sql
-- Power BI / Databricks SQL Analytics

SELECT 
    team,
    total_games,
    total_wins,
    total_draws,
    total_losses,
    total_points,
    total_goals_scored,
    total_goals_conceded,
    goal_difference,
    ROUND(total_points / NULLIF(total_games, 0), 2) as points_per_game,
    ROUND(total_goals_scored / NULLIF(total_games, 0), 2) as goals_per_game
FROM gold.fact_team_performance
WHERE championship = 'Brasileirão A'
  AND season = '2024/2025'
ORDER BY total_points DESC, goal_difference DESC, total_goals_scored DESC
LIMIT 20;
```

### 2. Times que Super/Sub Performam (xG Analysis)

```sql
-- Times que marcam mais que o esperado (super eficientes)

WITH team_xg_performance AS (
    SELECT 
        home_team as team,
        AVG(xg_diff_home) as avg_xg_overperformance,
        AVG(conversion_efficiency_home_pct) as avg_conversion_efficiency,
        COUNT(*) as matches
    FROM gold.fact_xg_analysis
    WHERE season = '2024/2025'
    GROUP BY home_team
    
    UNION ALL
    
    SELECT 
        away_team as team,
        AVG(xg_diff_away) as avg_xg_overperformance,
        AVG(conversion_efficiency_away_pct) as avg_conversion_efficiency,
        COUNT(*) as matches
    FROM gold.fact_xg_analysis
    WHERE season = '2024/2025'
    GROUP BY away_team
)

SELECT 
    team,
    ROUND(AVG(avg_xg_overperformance), 2) as xg_overperformance,
    ROUND(AVG(avg_conversion_efficiency), 2) as conversion_efficiency_pct,
    SUM(matches) as total_matches
FROM team_xg_performance
GROUP BY team
HAVING SUM(matches) >= 10  -- Mínimo de jogos
ORDER BY xg_overperformance DESC
LIMIT 10;
```

### 3. Análise de Confronto Direto (Head-to-Head)

```sql
-- Histórico Flamengo vs Palmeiras

SELECT 
    home_team,
    away_team,
    COUNT(*) as total_matches,
    SUM(CASE WHEN match_result = 'Home Win' THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN match_result = 'Away Win' THEN 1 ELSE 0 END) as away_wins,
    SUM(CASE WHEN match_result = 'Draw' THEN 1 ELSE 0 END) as draws,
    AVG(total_goals) as avg_goals_per_match,
    AVG(total_xg) as avg_xg_per_match,
    MAX(total_goals) as highest_scoring_match
FROM gold.dim_matches_complete
WHERE (home_team = 'Flamengo' AND away_team = 'Palmeiras')
   OR (home_team = 'Palmeiras' AND away_team = 'Flamengo')
GROUP BY home_team, away_team;
```

### 4. Análise de Eficiência Ofensiva

```sql
-- Times mais eficientes em converter chances

SELECT 
    team,
    championship,
    AVG(avg_shot_accuracy_pct) as shot_accuracy,
    AVG(avg_xg) as avg_xg_per_game,
    total_goals_scored,
    total_games,
    ROUND(total_goals_scored / NULLIF(total_games, 0), 2) as goals_per_game,
    ROUND(total_goals_scored / NULLIF(AVG(avg_xg), 0), 2) as goals_vs_xg_ratio
FROM gold.fact_team_performance
WHERE season = '2024/2025'
  AND total_games >= 10
ORDER BY shot_accuracy DESC
LIMIT 15;
```

### 5. Identificar Partidas com Surpresas (Upsets)

```sql
-- Partidas onde o resultado foi muito diferente do esperado

SELECT 
    match_datetime,
    championship,
    home_team,
    away_team,
    home_score,
    away_score,
    expected_goals_xg_home,
    expected_goals_xg_away,
    xg_diff_home,
    xg_diff_away,
    performance_classification,
    ABS(xg_diff_home) + ABS(xg_diff_away) as total_xg_surprise
FROM gold.fact_xg_analysis
WHERE season = '2024/2025'
  AND performance_classification IN ('Home Overperformed', 'Away Overperformed', 
                                     'Home Underperformed', 'Away Underperformed')
ORDER BY total_xg_surprise DESC
LIMIT 20;
```

### 6. Análise de Fator Casa

```sql
-- Performance em casa vs fora

SELECT 
    team,
    championship,
    games_home,
    games_away,
    wins_home,
    wins_away,
    ROUND(100.0 * wins_home / NULLIF(games_home, 0), 2) as win_rate_home,
    ROUND(100.0 * wins_away / NULLIF(games_away, 0), 2) as win_rate_away,
    goals_scored_home,
    goals_scored_away,
    ROUND(goals_scored_home / NULLIF(games_home, 0), 2) as goals_per_game_home,
    ROUND(goals_scored_away / NULLIF(games_away, 0), 2) as goals_per_game_away,
    avg_xg_home,
    avg_xg_away
FROM gold.fact_team_performance
WHERE season = '2024/2025'
  AND games_home >= 5
  AND games_away >= 5
ORDER BY (win_rate_home - win_rate_away) DESC;
```

### 7. Machine Learning - Feature Engineering

```python
# notebooks/ml_feature_engineering.ipynb

from pyspark.sql.functions import col, lag, avg as spark_avg
from pyspark.sql.window import Window

# Carregar dados
matches_df = spark.table("gold.dim_matches_complete")

# Window para últimos 5 jogos de cada time
window_spec = Window.partitionBy("home_team").orderBy("match_datetime").rowsBetween(-5, -1)

# Features de forma recente
features_df = (
    matches_df
    .withColumn("home_recent_avg_xg", spark_avg("expected_goals_xg_home").over(window_spec))
    .withColumn("home_recent_avg_goals", spark_avg("home_score").over(window_spec))
    .withColumn("home_recent_win_rate", 
                spark_avg(when(col("match_result") == "Home Win", 1).otherwise(0)).over(window_spec))
)

# Salvar features para ML
features_df.write.format("delta").mode("overwrite").saveAsTable("gold.ml_features_matches")

print("✅ Features criadas para ML!")
```

### 8. Dashboard Metrics - KPIs

```sql
-- Métricas para dashboard executivo

WITH season_stats AS (
    SELECT 
        COUNT(DISTINCT match_id) as total_matches,
        SUM(total_goals) as total_goals,
        AVG(total_goals) as avg_goals_per_match,
        AVG(total_xg) as avg_xg_per_match,
        AVG(possession_home) as avg_possession,
        COUNT(DISTINCT home_team) as total_teams
    FROM gold.dim_matches_complete
    WHERE season = '2024/2025'
)

SELECT 
    *,
    ROUND(total_goals / NULLIF(total_matches, 0), 2) as goals_per_match,
    ROUND(avg_xg_per_match, 2) as xg_per_match
FROM season_stats;
```

---

## 📈 Monitoramento e Observabilidade

### Data Quality Checks (DLT Expectations)

```python
# Exemplos de expectations implementadas

# Expectation de Drop (registros inválidos são descartados)
@dlt.expect_or_drop("valid_teams", "home_team IS NOT NULL AND away_team IS NOT NULL")
@dlt.expect_or_drop("valid_score", "home_score >= 0 AND away_score >= 0")

# Expectation de Warning (registros são mantidos mas alertados)
@dlt.expect_or_warn("realistic_possession", "possession_home + possession_away BETWEEN 95 AND 105")
@dlt.expect_or_warn("realistic_xg", "expected_goals_xg_home <= 10 AND expected_goals_xg_away <= 10")

# Expectation de Fail (pipeline falha se violado)
@dlt.expect_or_fail("match_id_unique", "COUNT(DISTINCT match_id) = COUNT(*)")
```

### Métricas do Pipeline

Acesse via **Databricks UI → Delta Live Tables → [Seu Pipeline]**

Métricas disponíveis:
- ✅ **Tempo de execução** por tabela e camada
- ✅ **Número de registros** processados
- ✅ **Taxa de erro** e data quality violations
- ✅ **Freshness** dos dados (última atualização)
- ✅ **Resource utilization** (DBU consumption)

### Logging

```python
# utils/logger.py

import logging
from datetime import datetime

def setup_logger(name, log_file):
    """
    Configura logger para scraping e upload
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger

# Uso no scraping
logger = setup_logger('scraping', f'logs/scraping_{datetime.now().date()}.log')
logger.info("Iniciando scraping...")
```

---

## 🔧 Configuração Avançada

### Otimização de Performance

```python
# Z-Ordering para queries frequentes
spark.sql("""
    OPTIMIZE gold.dim_matches_complete
    ZORDER BY (championship, season, match_datetime, home_team, away_team)
""")

# Vacuum para remover arquivos antigos (>7 dias)
spark.sql("""
    VACUUM gold.dim_matches_complete RETAIN 168 HOURS
""")

# Analyze table para estatísticas
spark.sql("""
    ANALYZE TABLE gold.dim_matches_complete COMPUTE STATISTICS
""")
```

### Particionamento

```python
# Particionar por season e championship
@dlt.table(
    name="gold.dim_matches_partitioned",
    partition_cols=["season", "championship"]
)
def matches_partitioned():
    return dlt.read("gold.dim_matches_complete")
```

### Scheduling Automático

```bash
# Databricks Jobs - via CLI
databricks jobs create --json '{
  "name": "Football Pipeline - Daily Update",
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "run_pipeline",
      "pipeline_task": {
        "pipeline_id": "YOUR_PIPELINE_ID",
        "full_refresh": false
      }
    }
  ],
  "email_notifications": {
    "on_failure": ["your_email@example.com"]
  }
}'
```

### Alertas

```python
# Configurar alertas em dlt_pipelines
@dlt.table(
    name="silver.fact_statistics"
)
@dlt.expect_or_fail("critical_data_quality", 
    """
    possession_home + possession_away BETWEEN 98 AND 102
    AND expected_goals_xg_home >= 0
    AND expected_goals_xg_away >= 0
    """)
def silver_statistics():
    # Se a expectation falhar, o pipeline para e envia alerta
    ...
```

---

## 🤝 Contribuindo

Contribuições são muito bem-vindas! Aqui está como você pode ajudar:

### Como Contribuir

1. **Fork** o projeto
2. **Clone** seu fork
   ```bash
   git clone https://github.com/seu-usuario/databricks_football_scraping.git
   ```
3. Crie uma **branch** para sua feature
   ```bash
   git checkout -b feature/MinhaNovaFeature
   ```
4. **Commit** suas mudanças
   ```bash
   git commit -m 'Add: descrição da feature'
   ```
5. **Push** para a branch
   ```bash
   git push origin feature/MinhaNovaFeature
   ```
6. Abra um **Pull Request**

### Guidelines de Contribuição

#### Código
- ✅ Siga PEP 8 para Python
- ✅ Adicione docstrings em funções e classes
- ✅ Mantenha código limpo e legível
- ✅ Use type hints quando possível

#### Commits
- Use mensagens descritivas
- Prefixos recomendados:
  - `Add:` para novas features
  - `Fix:` para correções
  - `Update:` para atualizações
  - `Refactor:` para refatoração
  - `Docs:` para documentação

#### Testes
- Adicione testes para novas funcionalidades
- Garanta que testes existentes passem
- Teste localmente antes do PR

#### Documentação
- Atualize o README se necessário
- Documente novas funcionalidades
- Inclua exemplos de uso

---

## 📝 Roadmap

### Em Desenvolvimento 🚧
- [ ] Dashboard interativo com Streamlit
- [ ] API REST para consultas aos dados
- [ ] Testes automatizados (pytest)
- [ ] CI/CD com GitHub Actions

### Planejado 📅
- [ ] Adicionar mais campeonatos (MLS, J-League, Liga MX)
- [ ] Implementar CDC (Change Data Capture)
- [ ] Machine Learning:
  - [ ] Modelo de predição de resultados
  - [ ] Modelo de predição de gols (Over/Under)
  - [ ] Análise de risco de apostas
- [ ] Integração com outras fontes de dados (API esportivas)
- [ ] Sistema de alertas em tempo real
- [ ] Mobile app para visualização

### Ideias Futuras 💡
- [ ] Análise de lesões e suspensões
- [ ] Previsão de escalações
- [ ] Análise de arbitragem
- [ ] Social media sentiment analysis
- [ ] Real-time live scores tracking

---

## 🐛 Troubleshooting

### Problemas Comuns

#### 1. ChromeDriver não encontrado
```bash
# Solução: Instale webdriver-manager
pip install webdriver-manager

# E use no código:
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
```

#### 2. Timeout no scraping
```python
# Aumente o timeout em utils/scraping_utils.py
def esperar_elemento(driver, selector, timeout=30):  # Aumentado para 30s
    ...
```

#### 3. Erro de autenticação Azure
```bash
# Verifique suas credenciais
az login
az account show

# Ou regenere o SAS token
```

#### 4. Pipeline DLT falhando
```python
# Verifique os logs no Databricks UI
# Delta Live Tables → [Pipeline] → Event Log

# Rode data quality checks manualmente
spark.sql("SELECT * FROM bronze.calendar WHERE home_team IS NULL")
```

#### 5. Performance lenta
```sql
-- Otimize tabelas grandes
OPTIMIZE gold.dim_matches_complete ZORDER BY (match_datetime, championship);

-- Limpe arquivos antigos
VACUUM gold.dim_matches_complete RETAIN 168 HOURS;
```

---

## 📄 Licença

Este projeto está sob a licença **MIT**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

```
MIT License

Copyright (c) 2024 Diogo Ribeiro

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 👨‍💻 Autor

<div align="center">

**Diogo Ribeiro**

Engenheiro de Dados | Azure & Databricks Specialist

[![GitHub](https://img.shields.io/badge/GitHub-DiogodsRibeiro-181717?style=for-the-badge&logo=github)](https://github.com/DiogodsRibeiro)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Diogo_Ribeiro-0077B5?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/diogo-ribeiro)
[![Email](https://img.shields.io/badge/Email-diogodsribeiro@gmail.com-D14836?style=for-the-badge&logo=gmail)](mailto:diogodsribeiro@gmail.com)

</div>

---

## 🙏 Agradecimentos

- **[Flashscore](https://www.flashscore.com.br)** - Fonte primária dos dados
- **[Databricks](https://www.databricks.com/)** - Plataforma de processamento lakehouse
- **[Microsoft Azure](https://azure.microsoft.com/)** - Infraestrutura cloud
- **[Selenium](https://www.selenium.dev/)** - Framework de web scraping
- **Comunidade Open Source** - Pelas bibliotecas e ferramentas incríveis

---

## 📊 Estatísticas do Projeto

<div align="center">

![GitHub last commit](https://img.shields.io/github/last-commit/DiogodsRibeiro/databricks_football_scraping)
![GitHub repo size](https://img.shields.io/github/repo-size/DiogodsRibeiro/databricks_football_scraping)
![GitHub stars](https://img.shields.io/github/stars/DiogodsRibeiro/databricks_football_scraping?style=social)
![GitHub forks](https://img.shields.io/github/forks/DiogodsRibeiro/databricks_football_scraping?style=social)

</div>

---

<div align="center">

### ⭐ Se este projeto foi útil para você, considere dar uma estrela!

### 📬 Dúvidas? Abra uma [Issue](https://github.com/DiogodsRibeiro/databricks_football_scraping/issues) ou me envie um email!

---

**Feito com ❤️, ☕ e muito ⚽ por [Diogo Ribeiro](https://github.com/DiogodsRibeiro)**

</div>
