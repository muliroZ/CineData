# CineData

Pipeline de engenharia de dados para análise da indústria cinematográfica, integrando dados do **TMDB**, **IMDb**, avaliações de usuários e cotações do **Banco Central (PTAX)** em uma arquitetura *medallion* no Databricks.

---

## Arquitetura

O projeto segue o padrão **Medallion Architecture** (Landing → Bronze → Silver) com tabelas Delta Lake gerenciadas pelo **Unity Catalog**.

```
┌──────────────┐     ┌───────────────────┐     ┌───────────────────┐
│   Landing     │     │     Bronze        │     │     Silver        │
│ (UC Volume)   │ ──▶ │ (Delta Tables)    │ ──▶ │ (Delta Tables)    │
│ Arquivos .csv │     │ Dados brutos +    │     │ Dados limpos,     │
│ + API PTAX    │     │ ingestion_datetime│     │ deduplicados e    │
│               │     │                   │     │ enriquecidos      │
└──────────────┘     └───────────────────┘     └───────────────────┘
```

### Notebooks

| Notebook | Função |
|----------|--------|
| **Landing_to_Bronze** | Ingestão de arquivos CSV e API PTAX para a camada Bronze |
| **Bronze_to_Silver** | Limpeza, padronização, deduplicação e enriquecimento para a camada Silver |

---

## Infraestrutura

| Recurso | Nome | Descrição |
|---------|------|-----------|
| Catalog | `cinedata` | Namespace raiz no Unity Catalog |
| Schema | `cinedata.landing` | Volume de landing |
| Schema | `cinedata.bronze` | Tabelas Delta da camada Bronze |
| Schema | `cinedata.silver` | Tabelas Delta da camada Silver |
| Volume | `cinedata.landing.landing_volume` | Armazenamento de arquivos .csv brutos |

---

## Tabelas

### Camada Bronze (ingestão bruta com `mode("append")`)

| Tabela | Origem | Conteúdo |
|--------|--------|---------|
| `tb_movies_info` | CSV | Dados cadastrais dos filmes |
| `tb_movies_financials` | CSV | Orçamento e receita |
| `tb_movies_metrics` | CSV | Popularidade e notas |
| `tb_credits_and_tags` | CSV | Elenco, gêneros, empresas |
| `tb_movies_reviews` | CSV | Avaliações de usuários |
| `tb_cotacao_dolar` | API PTAX | Cotação do dólar por dia |

### Camada Silver (dados limpos com `mode("overwrite")`)

| Tabela | Origem (Bronze) | Conteúdo |
|--------|------------------|---------|
| `tb_info_filmes` | `tb_movies_info` | Informações cadastrais em português |
| `tb_financeiro_filmes` | `tb_movies_financials` | Orçamento, receita e lucro em BRL |
| `tb_metricas_engajamento` | `tb_movies_metrics` | Popularidade e notas (TMDB / IMDb) |
| `tb_avaliacoes_usuarios` | `tb_movies_reviews` | Avaliações e comentários de usuários |
| `tb_generos` | `tb_credits_and_tags` (genres) | Gêneros associados aos filmes |
| `tb_pessoas_empresas` | `tb_credits_and_tags` | Pessoas e empresas envolvidas |
| `tb_cotacao_dolar` | `tb_cotacao_dolar` | Cotação do dólar com *forward fill* |

---

## Parâmetros do Notebook

Os notebooks utilizam *widgets* do Databricks para parametrização:

| Parâmetro | Formato | Uso |
|-----------|---------|-----|
| `data_inicio` | `MM-dd-yyyy` | Data inicial para a Silver `tb_cotacao_dolar` |
| `data_fim` | `MM-dd-yyyy` | Data final para a Silver `tb_cotacao_dolar` |
| `data_inicio_formatada` | `MM-dd-yyyy` | Data inicial para a API PTAX (Bronze) |
| `data_fim_formatada` | `MM-dd-yyyy` | Data final para a API PTAX (Bronze) |

---

## Técnicas de Engenharia de Dados

### Landing → Bronze

| # | Técnica | Descrição |
|---|--------|-----------|
| 1 | **Provisionamento UC** | `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` — infraestrutura idempotente |
| 2 | **Cópia para UC Volume** | `dbutils.fs.cp()` para governança de arquivos de landing |
| 3 | **Leitura CSV com `inferSchema`** | `spark.read.csv(header=True, inferSchema=True)` |
| 4 | ***Timestamping* de ingestão** | `current_timestamp()` em todos os DataFrames para auditoria e deduplicação |
| 5 | **Delta Lake `append`** | `mode("append")` para acúmulo de lotes históricos |
| 6 | **Ingestão via API REST** | `requests.get()` + `spark.createDataFrame()` para a API PTAX |

### Bronze → Silver

| # | Técnica | Descrição |
|---|--------|-----------|
| 1 | **Deduplicação com `Window` + `row_number`** | Mantém apenas o registro mais recente por chave de negócio |
| 2 | **Padronização de texto** | `trim` + `regexp_replace` + `initcap` para normalizar strings |
| 3 | **Conversão segura de tipos** | `try_cast` / `try_to_date` que retornam NULL em vez de falhar |
| 4 | **Tratamento de valores sentinela** | Substituição de `"UNKNOWN"`, `"N/A"`, `"[]"` por NULL |
| 5 | **Extração numérica com regex** | Conversão de sufixos (`M`, `K`) e remoção de não-numéricos |
| 6 | **Conversão monetária** | `coalesce` + `round` + proteção contra divisão por zero |
| 7 | **`.transform()` com funções auxiliares** | Encadeamento de transformações reutilizáveis |
| 8 | **`explode` / `split`** | Normalização de listas separadas por delimitadores |
| 9 | **Filtragem de *column shift*** | `left_anti join` para remover valores de domínio incorreto |
| 10 | **Validação com regex Unicode** | `\p{L}`, `\p{N}` para preservar acentos e caracteres internacionais |
| 11 | **Capitalização inteligente** | Preserva acrônimos em maiúsculas (`NASA`, `IBM`) |
| 12 | **`dropDuplicates`** | Deduplicação semântica quando não há *timestamp* |
| 13 | ***Forward fill* com `Window`** | Propagação do último valor conhecido para séries temporais |
| 14 | **Persistência Delta `overwrite`** | `mode("overwrite")` com `overwriteSchema` para reprocessamento completo |

> Documentação detalhada de cada técnica está nas células markdown iniciais de cada notebook.

---

## Execução

1. Configure os parâmetros `data_inicio`, `data_fim`, `data_inicio_formatada` e `data_fim_formatada` na barra de parâmetros
2. Execute o notebook **Landing_to_Bronze** para ingerir os dados
3. Execute o notebook **Bronze_to_Silver** para processar e enriquecer os dados

---

## Tecnologias

- **Databricks** (Serverless Compute)
- **Apache Spark** / **PySpark** (processamento distribuído)
- **Delta Lake** (transações ACID, *time travel*)
- **Unity Catalog** (governança e segurança)
- **Python** (`requests`, `pyspark.sql`)