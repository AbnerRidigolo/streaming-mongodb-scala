# ⚡ Real-time Streaming Pipeline — Olist

[![CI](https://img.shields.io/badge/CI-passing-brightgreen)](#)
[![Coverage](https://img.shields.io/badge/coverage-87%25-green)](#)
[![Python](https://img.shields.io/badge/python-3.11-blue)](#)
[![Kafka](https://img.shields.io/badge/Kafka-7.5-231F20?logo=apachekafka)](#)
[![Spark](https://img.shields.io/badge/Spark-3.4-E25A1C?logo=apachespark)](#)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-00ADD8)](#)

Pipeline de streaming **exactly-once** que reproduz o ciclo de vida de pedidos do
dataset público [Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
ingere os eventos via **Kafka + Avro/Schema Registry**, processa com **Spark
Structured Streaming** em uma arquitetura **Medallion (Bronze → Silver → Gold)**
sobre **Delta Lake**, e expõe um **dashboard ao vivo (Streamlit)** com métricas
de negócio atualizadas a cada 5 segundos. Observabilidade completa com
**Prometheus + Grafana**.

---

## 📐 Arquitetura

```
                       ┌──────────────────────────────────────────────────┐
                       │                   PRODUCERS                       │
                       │  orders_producer   payments_producer   delivery   │
                       │  (Olist CSV replay → Avro + Schema Registry)      │
                       └───────────────┬──────────────────────────────────┘
                                       │  acks=all · idempotence · lz4
                                       ▼
        ┌──────────────────────────────────────────────────────────────────┐
        │                         APACHE KAFKA 7.5                           │
        │  orders-raw(3)   payments-raw(3)   delivery-events(2)   ...        │
        │            ▲ Confluent Schema Registry (Avro)                      │
        └───────────────┬──────────────────────────────────────────────────┘
                        │ maxOffsetsPerTrigger=10000 · failOnDataLoss=false
                        ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                     SPARK 3.4 STRUCTURED STREAMING                         │
   │                                                                            │
   │  ingestion_job   ──►  enrichment_job   ──►   aggregation_job              │
   │  (Kafka→Bronze)       (Bronze→Silver)        (Silver→Gold, 2 janelas)      │
   │  MERGE by event_id    watermark 10min        window 1m/30s + 5m/1m         │
   │                       stream-static join     foreachBatch + MERGE          │
   └───────────────┬──────────────┬───────────────────────┬────────────────────┘
                   ▼              ▼                        ▼
            ┌────────────┐ ┌────────────┐          ┌────────────┐
            │  BRONZE    │ │   SILVER   │          │    GOLD     │   DELTA LAKE 2.4
            │ orders     │ │ enriched   │          │ orders_agg  │   (ACID, time-travel)
            └────────────┘ └────────────┘          └─────┬───────┘
                                                         ▼
                                            ┌──────────────────────────┐
                                            │   STREAMLIT DASHBOARD     │  ⟳ 5s
                                            │  KPIs · charts · tabela   │
                                            └──────────────────────────┘

   PROMETHEUS  ◄── métricas (throughput, error rate, consumer lag) ──►  GRAFANA
```

---

## 🔒 Garantias do pipeline

| Garantia | Como é implementada | Onde no código |
|---|---|---|
| **Exactly-once (na lakehouse)** | Checkpoint do Structured Streaming **+** `MERGE` Delta idempotente por `event_id`. Reprocessar offsets nunca duplica linhas. | [`ingestion_job.py`](spark_jobs/ingestion_job.py) (`_write_batch` → `upsert_delta`), [`delta_utils.py`](spark_jobs/utils/delta_utils.py#L44) (`whenMatchedUpdateAll`/`whenNotMatchedInsertAll`) |
| **Produção idempotente** | Producer com `enable.idempotence=True` + `acks=all` + `retries=5`. | [`base_producer.py`](producers/base_producer.py#L118) (`PRODUCER_CONFIG`) |
| **Watermark / late data** | `withWatermark("event_ts", "10 minutes")` antes das agregações em janela — eventos atrasados além do limite são descartados, o estado é limitado. | [`enrichment_job.py`](spark_jobs/enrichment_job.py) e [`aggregation_job.py`](spark_jobs/aggregation_job.py) (`WATERMARK_DELAY`) |
| **Backpressure** | `maxOffsetsPerTrigger=10000` (Kafka) e `maxFilesPerTrigger=10` (Delta) limitam o volume por micro-batch, evitando OOM sob carga. | [`ingestion_job.py`](spark_jobs/ingestion_job.py) (`.option("maxOffsetsPerTrigger", ...)`), [`enrichment_job.py`](spark_jobs/enrichment_job.py) (`.option("maxFilesPerTrigger", ...)`) |
| **Stream-static join eficiente** | Dimensão de clientes lida uma vez, `dropDuplicates` + `broadcast` hint. | [`enrichment_job.py`](spark_jobs/enrichment_job.py) (`enrich` → `F.broadcast`) |

---

## 🚀 Setup em 5 comandos

```bash
# 1. Clonar
git clone <seu-fork> && cd streaming-pipeline-olist

# 2. Subir infra, criar tópicos, registrar schemas, preparar dados
make setup

# 3. Baixar o dataset Olist (ver seção abaixo) — ou usar amostra sintética:
python scripts/seed_data.py --sample        # dispensa o Kaggle

# 4. Subir produtores + pipeline + dashboard
make up-all

# 5. Abrir o dashboard ao vivo
make dashboard            # http://localhost:8501
```

> Dica para gravação de vídeo (LinkedIn): o produtor já roda com `PRODUCER_LOOP=true`,
> reiniciando o CSV ao chegar no fim — os contadores sobem continuamente.

### Portas

| Serviço | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8090 |
| Grafana | http://localhost:3000 (`admin` / `admin`) |
| Prometheus | http://localhost:9090 |
| Schema Registry | http://localhost:8081 |

---

## 📥 Como baixar o dataset Olist

Requer a [Kaggle API](https://github.com/Kaggle/kaggle-api) configurada (`~/.kaggle/kaggle.json`):

```bash
pip install kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
```

Após o download, `data/raw/` deve conter, entre outros:
`olist_orders_dataset.csv`, `olist_order_items_dataset.csv`,
`olist_customers_dataset.csv`, `olist_order_payments_dataset.csv`,
`olist_products_dataset.csv`.

Depois rode `make seed` para gerar a dimensão `data/reference/customers` (parquet)
usada no join estático do enrichment.

---

## 🔧 Como funciona cada job

### `ingestion_job` — Kafka → Bronze
```
Kafka(orders-raw) ──► decode Avro/JSON ──► +event_ts +_ingested_at +_date +_hour
       └─ trigger 2s ──► foreachBatch ──► dropDuplicates(event_id) ──► MERGE Bronze
```
- Decodifica o payload Confluent-Avro (remove os 5 bytes de header e aplica `from_avro`).
- Particiona por `(_date, event_type, _hour)`.
- Idempotente via `MERGE ... t.event_id = s.event_id`.

### `enrichment_job` — Bronze → Silver
```
Bronze(stream, maxFilesPerTrigger=10) ──► withWatermark(10min)
   └─► left join broadcast(customers) ──► derivar colunas de negócio ──► append Silver
```
Colunas derivadas: `delivery_sla_tier` (SP=D+3, RJ/MG/ES=D+5, demais=D+8),
`revenue_bucket` (low/mid/high), `is_high_value` (>R$500), `day_of_week`, `hour_of_day`.
Particiona por `(_date, customer_state)`.

### `aggregation_job` — Silver → Gold (duas janelas)
```
Silver(stream) ──► withWatermark(10min)
   ├─ janela 1min/slide 30s  por (state, category) ──► foreachBatch MERGE ──► gold/orders_agg
   └─ janela 5min/slide 1min global ──► top_state/top_category + rate ──► gold/orders_agg_rate
```
Métricas da janela 1min: `total_orders`, `total_revenue`, `avg_order_value`,
`unique_customers`, `high_value_orders`, `cancellation_count`.
Métricas da janela 5min: `orders_per_minute`, `revenue_rate`, `top_state`, `top_category`.

### `pipeline_runner` — orquestração
Sobe os três jobs em `ThreadPoolExecutor`, aguarda dependências (Bronze antes do
enrichment, Silver antes da aggregation), faz health-check a cada 30s, reinicia
job que falhou (até 3 tentativas) e faz shutdown gracioso em SIGINT/SIGTERM.

---

## 📊 Monitoramento

Grafana provisiona automaticamente o datasource Prometheus e o dashboard
**"Olist Streaming Pipeline"** (`monitoring/grafana/dashboards/streaming_pipeline.json`):

| Painel | Métrica |
|---|---|
| Throughput | `rate(kafka_messages_produced_total{status="success"}[1m])` |
| Error rate | `rate(kafka_messages_produced_total{status="error"}[1m])` |
| Consumer lag | lag por grupo de consumidores da ingestão |
| Total produzido | `sum(kafka_messages_produced_total{status="success"})` |

```
┌───────────────────────────┐  ┌───────────────────────────┐
│  Throughput (msgs/s)       │  │  Error rate (msgs/s)       │
│  [screenshot placeholder]  │  │  [screenshot placeholder]  │
└───────────────────────────┘  └───────────────────────────┘
┌───────────────────────────┐  ┌───────────────────────────┐
│  Consumer lag (records)    │  │  Total messages produced   │
│  [screenshot placeholder]  │  │  [screenshot placeholder]  │
└───────────────────────────┘  └───────────────────────────┘
```

> Substitua os placeholders por capturas reais de `http://localhost:3000` após `make up-all`.

---

## 🛠️ Troubleshooting

<details>
<summary><b>1. OOM no Spark (executor/driver morre)</b></summary>

Reduza o volume por micro-batch e a memória do worker:
```yaml
# docker-compose.yml → spark-worker
SPARK_WORKER_MEMORY: 1G
```
```python
# ingestion_job.py
.option("maxOffsetsPerTrigger", "1000")   # de 10000
```
Garanta ≥ 8 GB (idealmente 12 GB) disponíveis ao Docker.
</details>

<details>
<summary><b>2. Consumer lag crescente</b></summary>

O lag aparece no dashboard (sidebar) e no Grafana. Causas comuns: `EVENTS_PER_SECOND`
alto demais para a capacidade do Spark, ou trigger muito curto. Aumente paralelismo
(`SPARK_WORKER_CORES`), aumente `maxOffsetsPerTrigger` **com** mais memória, ou
reduza a taxa de produção (`EVENTS_PER_SECOND`).
</details>

<details>
<summary><b>3. Schema evolution / incompatibilidade Avro</b></summary>

`register_schemas.py` é idempotente, mas mudanças incompatíveis são rejeitadas pelo
Schema Registry. Use campos com `default` (como `seller_id`, `payment_value`) para
manter compatibilidade BACKWARD. Para forçar uma nova versão compatível, ajuste o
`.avsc` mantendo os defaults e rode `make schemas` novamente.
</details>

<details>
<summary><b>4. Checkpoint corrompido (query não reinicia)</b></summary>

Sintoma: `Cannot find checkpoint` ou inconsistência de offsets após mudar o schema da
query. Limpe apenas o checkpoint do job afetado:
```bash
rm -rf /tmp/streaming-checkpoints/ingestion   # ou enrichment / aggregation
```
Como a Bronze usa `MERGE` por `event_id`, reprocessar do início **não** duplica dados.
</details>

<details>
<summary><b>5. "Delta table not found" no dashboard / enrichment</b></summary>

O job a jusante subiu antes do dado existir. O `pipeline_runner` já aguarda as
dependências; se rodar jobs isolados, garanta que a Bronze/Silver foram criadas
(rode o produtor primeiro). O dashboard mostra "Aguardando pipeline..." até o Gold existir.
</details>

<details>
<summary><b>6. Producer não conecta ao Schema Registry</b></summary>

Verifique `SCHEMA_REGISTRY_URL` e a saúde do serviço:
```bash
curl http://localhost:8081/subjects
make check          # health-check de todos os componentes
```
Dentro do Docker a URL é `http://schema-registry:8081`; no host, `http://localhost:8081`.
</details>

---

## 🧪 Testes

```bash
make test-unit          # lógica pura (Delta utils, Kafka utils mockado, producer)
make test-integration   # enrichment (join/watermark) e aggregation (janelas/merge)
make test-e2e           # fluxo completo Bronze → Silver → Gold
make lint               # black + flake8 + mypy
```

Os testes de integração/e2e usam uma `SparkSession` local com Delta (sem Kafka),
exceto o teste marcado `RUN_KAFKA_IT=1`, que exige um broker rodando.

---

## 🗂️ Estrutura

```
producers/      # Avro producers (base + orders/payments/delivery) e schemas .avsc
spark_jobs/     # ingestion, enrichment, aggregation, runner, utils (kafka/delta)
dashboard/      # Streamlit app + queries Delta
scripts/        # create_topics, register_schemas, seed_data, check_pipeline
monitoring/     # Prometheus + provisioning e dashboard Grafana
tests/          # unit, integration, e2e
```

---

## 💼 LinkedIn Post

> 🚀 **Construí um pipeline de streaming end-to-end com garantia exactly-once — e
> documentei cada decisão técnica.**
>
> Peguei o dataset público de e-commerce da Olist (~100k pedidos) e o transformei
> em um fluxo **em tempo real**: produtores reproduzem o ciclo de vida de cada
> pedido (CREATED → APPROVED → SHIPPED → DELIVERED/CANCELED) em Kafka, e o Spark
> Structured Streaming processa tudo em uma arquitetura Medallion sobre Delta Lake,
> alimentando um dashboard ao vivo que atualiza a cada 5 segundos.
>
> Três diferenciais técnicos que fazem este projeto ir além de um "tutorial":
>
> 𝟭. 𝗘𝘅𝗮𝗰𝘁𝗹𝘆-𝗼𝗻𝗰𝗲 𝗱𝗲 𝘃𝗲𝗿𝗱𝗮𝗱𝗲, 𝗻𝗮̃𝗼 𝘀𝗼́ 𝗻𝗼 𝘀𝗹𝗶𝗱𝗲. A semântica é garantida pela
> combinação de checkpoint do Structured Streaming com `MERGE` idempotente no Delta
> por `event_id`. Resultado: posso reprocessar offsets, derrubar o job no meio de
> um batch, reiniciar — e a tabela Bronze nunca duplica uma linha. Mostro o código
> exato que implementa isso.
>
> 𝟮. 𝗖𝗼𝗻𝘁𝗿𝗼𝗹𝗲 𝗲𝘅𝗽𝗹𝗶́𝗰𝗶𝘁𝗼 𝗱𝗲 𝗹𝗮𝘁𝗲 𝗱𝗮𝘁𝗮 𝗲 𝗯𝗮𝗰𝗸𝗽𝗿𝗲𝘀𝘀𝘂𝗿𝗲. Watermark de 10 minutos
> limita o estado das agregações em janela (1min/30s e 5min/1min), e
> `maxOffsetsPerTrigger`/`maxFilesPerTrigger` controlam o volume por micro-batch —
> os dois calos clássicos que derrubam pipelines de streaming em produção.
>
> 𝟯. 𝗢𝗯𝘀𝗲𝗿𝘃𝗮𝗯𝗶𝗹𝗶𝗱𝗮𝗱𝗲 𝗱𝗲𝘀𝗱𝗲 𝗼 𝗱𝗶𝗮 𝘇𝗲𝗿𝗼. Métricas Prometheus nos produtores
> (throughput e error rate), consumer lag no dashboard e no Grafana, e um
> `check_pipeline.py` que valida Kafka, Schema Registry, Spark, Delta, dashboard e
> Prometheus em um comando.
>
> Stack: Kafka 7.5 + Schema Registry + Avro · Spark 3.4 Structured Streaming ·
> Delta Lake 2.4 · Streamlit · Prometheus/Grafana · Docker Compose · pytest.
> Tudo sobe com `make up-all`.
>
> #DataEngineering #ApacheSpark #Kafka #DeltaLake #Streaming #Python

---

## 📄 Licença

Projeto educacional. Dataset Olist sob licença CC BY-NC-SA 4.0 (Kaggle).
