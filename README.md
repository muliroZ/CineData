# CineData

Pipeline de engenharia de dados para análise da indústria cinematográfica, integrando dados do **TMDB**, **IMDb**, avaliações de usuários e cotações do **Banco Central (PTAX)** em uma arquitetura *medallion* (Landing → Bronze → Silver → Gold) no Databricks, com modelagem dimensional na camada Gold e geração de documentos contextuais para GenAI.

---

## Arquitetura

O projeto segue o padrão **Medallion Architecture** (Landing → Bronze → Silver → Gold) com tabelas Delta Lake gerenciadas pelo **Unity Catalog**.

```
┌──────────────┐     ┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│   Landing     │     │     Bronze        │     │     Silver        │     │      Gold         │
│ (UC Volume)   │ ──▶ │ (Delta Tables)    │ ──▶ │ (Delta Tables)    │ ──▶ │ (Delta Tables)    │
│ Arquivos .csv │     │ Dados brutos +    │     │ Dados limpos,     │     │ Modelagem star    │
│ + API PTAX    │     │ ingestion_datetime│     │ deduplicados e    │     │ (dim, bridge,     │
│               │     │                   │     │ enriquecidos      │     │ fact) + GenAI     │
└──────────────┘     └───────────────────┘     └───────────────────┘     └───────────────────┘
```

### Notebooks

| Notebook | Função |
|----------|--------|
| **Landing_to_Bronze** | Ingestão de arquivos CSV e API PTAX para a camada Bronze |
| **Bronze_to_Silver** | Limpeza, padronização, deduplicação e enriquecimento para a camada Silver |
| **Silver_to_Gold** | Modelagem dimensional (star schema): dimensões, pontes, fato e tabela GenAI |

---

## Infraestrutura

| Recurso | Nome | Descrição |
|---------|------|-----------|
| Catalog | `cinedata` | Namespace raiz no Unity Catalog |
| Schema | `cinedata.landing` | Volume de landing |
| Schema | `cinedata.bronze` | Tabelas Delta da camada Bronze |
| Schema | `cinedata.silver` | Tabelas Delta da camada Silver |
| Schema | `cinedata.gold` | Tabelas Delta da camada Gold (star schema) |
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

### Camada Gold (modelagem dimensional com `mode("overwrite")`)

#### Dimensões

| Tabela | Origem (Silver) | Conteúdo |
|--------|-----------------|---------|
| `dim_movies` | `tb_info_filmes` | Cadastro de filmes com SK determinística (sha256) |
| `dim_genres` | `tb_generos` | Catálogo único de gêneros com SK |
| `dim_people` | `tb_pessoas_empresas` | Atores, diretores e roteiristas com SK |
| `dim_companies` | `tb_pessoas_empresas` | Produtoras com SK |
| `dim_reviews` | `tb_avaliacoes_usuarios` | Agregação de avaliações por filme (qtd + nota média) |

#### Pontes (Bridge)

| Tabela | Origem | Conteúdo |
|--------|--------|---------|
| `bridge_movie_genre` | `tb_generos` | Relação N:N entre filmes e gêneros via SKs |
| `bridge_movie_person` | `tb_pessoas_empresas` | Relação N:N entre filmes e pessoas via SKs |
| `bridge_movie_company` | `tb_pessoas_empresas` | Relação N:N entre filmes e produtoras via SKs |

#### Fato

| Tabela | Origem (Silver) | Conteúdo |
|--------|------------------|---------|
| `fact_movies_performance` | `tb_info_filmes` + `tb_financeiro_filmes` + `tb_metricas_engajamento` | Orçamento, receita, lucro (USD/BRL), popularidade e notas (TMDB/IMDb) |

#### GenAI

| Tabela | Origem (Gold) | Conteúdo |
|--------|---------------|---------|
| `genai_movies_context` | `dim_movies` + `fact_movies_performance` + `bridge_movie_person` | Documento contextual em português para uso em LLM/RAG |

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

### Silver → Gold

| # | Técnica | Descrição |
|---|--------|-----------|
| 1 | **Surrogate Keys determinísticas** | `sha2` + `conv` (hex → bigint) — SKs calculadas sem custo de join |
| 2 | **Modelagem star schema** | Separação em `dim`, `bridge` e `fact` seguindo boas práticas dimensionais |
| 3 | **Catálogo de gêneros** | `dropDuplicates` para criar dimensão única de gêneros |
| 4 | **Filtragem por tipo de entidade** | `isin` para pessoas (Ator/Diretor/Roteirista); filtro de Produtora para empresas |
| 5 | **Agregação de avaliações** | `groupBy` + `count` + `avg` para resumir avaliações por filme |
| 6 | **Pontes N:N** | `select` de SKs + `distinct()` para relações muitos-para-muitos |
| 7 | **Fato com múltiplos joins** | `left join` de info + financeiro + métricas filtrando status "Lançado" |
| 8 | **`try_cast` para tipos monetários** | `try_cast` para `decimal(18,2)` e `double` com segurança |
| 9 | **Documento contextual GenAI** | `format_string` + `coalesce` + `concat_ws` + `collect_set` para gerar texto narrativo |
| 10 | **Persistência Delta `overwrite`** | `mode("overwrite")` para reprocessamento completo da camada Gold |

---

## Execução

1. Configure os parâmetros `data_inicio`, `data_fim`, `data_inicio_formatada` e `data_fim_formatada` na barra de parâmetros
2. Execute o notebook **Landing_to_Bronze** para ingerir os dados
3. Execute o notebook **Bronze_to_Silver** para processar e enriquecer os dados
4. Execute o notebook **Silver_to_Gold** para construir a modelagem dimensional e a tabela GenAI

---

## Tecnologias

- **Databricks** (Serverless Compute)
- **Apache Spark** / **PySpark** (processamento distribuído)
- **Delta Lake** (transações ACID, *time travel*)
- **Unity Catalog** (governança e segurança)
- **Python** (`requests`, `pyspark.sql`)