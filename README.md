# ⚡ Real-time Streaming Pipeline — Olist

[![CI](https://github.com/AbnerRidigolo/streaming-mongodb-scala/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AbnerRidigolo/streaming-mongodb-scala/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11-blue)](#)
[![Java](https://img.shields.io/badge/Java-17-007396)](#)
[![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala)](#)
[![Confluent Platform](https://img.shields.io/badge/Confluent%20Platform-7.5%20(Kafka%203.5)-231F20?logo=apachekafka)](#)
[![Spark](https://img.shields.io/badge/Spark-3.4-E25A1C?logo=apachespark)](#)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-00ADD8)](#)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb)](#)

Pipeline de streaming que reproduz o ciclo de vida de pedidos do dataset público
[Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
em **Kafka** (Avro + Schema Registry) e o processa por três caminhos:

- **Spark Structured Streaming em Python**: medalhão Bronze → Silver → Gold em
  **Delta Lake**, com um painel **Streamlit** de métricas por janela.
- **Kafka Streams em Java** (`order-status-service`): estado atual de cada pedido e
  alertas de regra de negócio, levados ao **MongoDB** por um **Kafka Connect** sink.
- **Spark Structured Streaming em Scala** (`delivery-sla-job`): linha do tempo de
  entrega por pedido e SLA contra a data prometida, gravados no **MongoDB** e no Delta.

Tudo roda localmente com Docker. **Prometheus + Grafana** acompanham produtores,
Spark e o serviço Java.

---

## 📐 Arquitetura

```
                  Olist CSV (dataset real ou amostra sintética), replay em loop
        ┌──────────────────────────────┼──────────────────────────────┐
  orders_producer              payments_producer              delivery_producer     Python · Avro
        ▼                              ▼                              ▼             + Schema Registry
  orders-raw (3 part.)          payments-raw (3)              delivery-events (2)   Kafka 3.5 (CP 7.5)
        │                              │                              │
        ├─────────────────────┐        │                              │
        ▼                     │        │                              │
 ┌──────────────────────┐     │        │                              │
 │ spark-pipeline (Py)  │     ▼        ▼                              ▼
 │ Bronze → Silver →    │   ┌──────────────────────────────────────────────────┐
 │ Gold (Delta Lake)    │   │ order-status-service (Java, Kafka Streams)        │
 └──────────┬───────────┘   │ cogroup por order_id → KTable · exactly_once_v2   │
            ▼               │ GET /orders/{id} (interactive query)              │
   Streamlit: painel        └──────────────┬──────────────────────┬────────────┘
   principal (Gold)                        ▼                      ▼
                                 order-status (compactado)   order-alerts
                                           └──────────┬───────────┘
                                                      ▼
                                   Kafka Connect · MongoDB Sink (upsert por chave, DLQ)
                                                      │
 orders-raw + delivery-events                         ▼
        │                          ┌───────────────── MongoDB 7.0 · olist_serving ─────────────────┐
        ▼                          │ order_status · order_alerts   (caminho Kafka Streams + Connect)│
 ┌──────────────────────────┐      │ delivery_timeline · delivery_sla   (caminho Scala)             │
 │ delivery-sla-job (Scala) │ ───► └──────────────────────────────┬─────────────────────────────────┘
 │ flatMapGroupsWithState   │ ───► Delta gold                     ▼
 └──────────────────────────┘      delivery_timeline     Streamlit: "Consulta de pedido"
                                                          (usuário só-leitura)

 Prometheus ◄── produtores · Spark (Python) · order-status-service ──► Grafana
```

### Componentes

| Componente | Tecnologia | Papel | Detalhes |
|---|---|---|---|
| Produtores | Python 3.11, confluent-kafka, Avro | Reproduzem os CSVs da Olist em três tópicos, cada um no seu ritmo | [`producers/`](producers/) |
| `init` | Python (one-shot) | Cria tópicos, registra schemas, gera a amostra sintética se não houver CSVs | [`scripts/`](scripts/) |
| `spark-pipeline` | PySpark 3.4, Delta 2.4 | Medalhão Bronze → Silver → Gold com janelas | [jobs Python](#-como-funciona-cada-job) |
| `dashboard` | Streamlit | Painel do Gold e página de consulta de pedido no MongoDB | [`dashboard/`](dashboard/) |
| `order-status-service` | Java 17, Kafka Streams 3.5 | Estado atual por pedido e alertas, API HTTP | [seção](#-order-status-service--kafka-streams-java) |
| `connect` + `connect-init` | Kafka Connect, MongoDB Sink 3.1.2 | Leva `order-status` e `order-alerts` ao MongoDB | [seção](#-camada-de-serviço--mongodb-via-kafka-connect) |
| `mongo` | MongoDB 7.0 | Camada de serviço (`olist_serving`), usuários de menor privilégio | [seção](#-camada-de-serviço--mongodb-via-kafka-connect) |
| `delivery-sla-job` | Scala 2.12, Spark 3.4, MongoDB Spark Connector 10.4 | Linha do tempo de entrega e SLA por UF/região | [seção](#-delivery-sla-job--linha-do-tempo-de-entrega-e-sla-scala) |
| Prometheus + Grafana | | Métricas de produtores, Spark e serviço Java | [seção](#-monitoramento) |

---

## 🚀 Como rodar

Só precisa de Docker (com 10 GB ou mais de RAM para ele; a stack completa usa
~8 GiB, ver [consumo medido](#consumo-de-memória-medido)) e Git. No Windows, rode os
scripts `.sh` pelo Git Bash. Funciona igual no Windows, macOS e Linux.

```bash
git clone https://github.com/AbnerRidigolo/streaming-mongodb-scala.git
cd streaming-mongodb-scala
scripts/up.sh             # gera o .env se faltar e sobe tudo (docker compose up -d --build)
scripts/smoke_e2e.sh      # opcional: confere os dois caminhos até o MongoDB
```

O MongoDB exige senhas, e elas não ficam no repositório: `scripts/up.sh` chama
`scripts/gen_env.sh`, que cria (ou completa) o `.env`, ignorado pelo git, com senhas
aleatórias e nunca as imprime. Quem preferir o compose direto roda
`scripts/gen_env.sh` uma vez e depois `docker compose up -d --build`; sem o `.env`, o
compose para com `required variable MONGO_ROOT_PASSWORD is missing a value: missing in .env, run scripts/gen_env.sh`.
Com `make`, `make up-all` faz o mesmo que `scripts/up.sh`.

O serviço `init` roda uma vez antes de produtores e pipeline: cria os tópicos,
registra os schemas Avro e prepara os dados. Se `data/raw/` tiver os CSVs reais da
Olist (ver [seção abaixo](#-como-baixar-o-dataset-olist)), eles são usados; senão,
gera uma amostra sintética de 10.000 pedidos. Os primeiros dados aparecem no
MongoDB em cerca de um minuto e no painel principal depois que o Gold é escrito.

**Verificação ponta a ponta.** `scripts/smoke_e2e.sh` (o mesmo do CI) sobe a parte
da stack que escreve no MongoDB e confere os dois caminhos: o pedido `order_000000`
com o mesmo status na API do Kafka Streams e no MongoDB, alertas em `order_alerts`,
DLQs sem registros novos, o usuário do painel sem permissão de escrita, e a linha do
tempo de entrega do mesmo pedido, com data estimada e SLA, vinda do job Scala. Num
ambiente limpo levou 1 min 30 s aqui.

> **Windows, clone feito antes do `.gitattributes`:** se o build falhar com
> `./gradlew: not found`, os arquivos foram extraídos com CRLF (padrão do Git for
> Windows). Com a árvore limpa, rode `git rm -r --cached . && git reset --hard`
> para extraí-los de novo com LF.

### Portas

| Serviço | URL |
|---|---|
| Painel Streamlit | http://localhost:8501 |
| Consulta de pedido (MongoDB) | http://localhost:8501/Consulta_de_pedido |
| order-status-service (Kafka Streams) | http://localhost:8088/orders/{order_id} |
| Kafka Connect (REST) | http://localhost:8083/connectors?expand=status |
| Grafana | http://localhost:3000 (`admin` / `admin`) |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| Spark UI (pipeline Python) | http://localhost:4040 |
| Spark UI (delivery-sla-job, Scala) | http://localhost:4041 |
| MongoDB | `mongodb://localhost:27018` (só em 127.0.0.1; porta em `MONGO_HOST_PORT`) |

---

## 📸 O que se vê rodando

Capturas de 2026-10-09, numa stack recém-criada (`docker compose down -v`) com a
amostra sintética. As telas foram tiradas por um Chromium headless na rede do
compose; os textos são saídas do `scripts/smoke_e2e.sh` da mesma execução.

**API do `order-status-service`** (`GET /orders/order_000000`, interactive query na store do Kafka Streams):

![GET /orders/order_000000](docs/img/api-order-status.png)

**MongoDB, caminho Kafka Streams + Connect e caminho Scala.** O mesmo pedido em
`delivery_timeline` e o SLA por região em `delivery_sla`:

```js
// db.delivery_timeline.findOne({_id: 'order_000000'})
{
  _id: 'order_000000', order_id: 'order_000000', customer_state: 'SP', region: 'Sudeste',
  delivery_status: 'DELIVERED', sla_status: 'ON_TIME',
  delay_days: -5.11, promised_days: 15.74, actual_days: 10.63,
  purchase_ts: ISODate('2024-03-13T06:20:00.000Z'),
  estimated_delivery_ts: ISODate('2024-03-29T00:00:00.000Z'),
  delivered_customer_ts: ISODate('2024-03-23T21:20:00.000Z'),
  created_at: ISODate('2026-10-09T00:51:37.000Z'), shipped_at: ISODate('2026-10-09T00:51:36.959Z'),
  in_transit_at: ISODate('2026-10-09T00:51:37.069Z'), out_for_delivery_at: ISODate('2026-10-09T00:51:37.169Z'),
  delivered_at: ISODate('2026-10-09T00:51:37.270Z'), canceled_at: null,
  last_carrier_status: 'DELIVERED', ...
}
```

```
== MongoDB olist_serving.delivery_sla (country and regions)   (início do stream)
all:BR delivered: 78 late: 4 on_time_rate: 0.9487 avg_delay_days_when_late: 6.75
region:Centro-Oeste delivered: 15 late: 2 on_time_rate: 0.8667 avg_delay_days_when_late: 7.13
region:Nordeste delivered: 16 late: 1 on_time_rate: 0.9375 avg_delay_days_when_late: 7.88
region:Sudeste delivered: 26 late: 1 on_time_rate: 0.9615 avg_delay_days_when_late: 4.88
region:Sul delivered: 21 late: 0 on_time_rate: 1 avg_delay_days_when_late: null
PASS: path 1 (Kafka Streams + Connect): order_000000 is DELIVERED in the API and in MongoDB; path 2 (Scala job): its delivery SLA is ON_TIME
```

Com os 10.000 pedidos da amostra processados, numa execução anterior, o SLA do
país ficou em 89,96% no prazo (8.754 entregues), coerente com os ~10% de atraso
que o gerador da amostra sorteia.

**Página "Consulta de pedido"** (lê `order_status` e `order_alerts` no MongoDB com o usuário só-leitura):

![Consulta de pedido](docs/img/dashboard-consulta-pedido.png)

**Painel principal** (Gold do pipeline Python):

![Painel principal](docs/img/dashboard-principal.png)

**Grafana** (produtores, ingestão do Spark e `order-status-service`; o painel de
erro fica vazio porque nenhum envio falhou e a série de erro não existe):

![Grafana](docs/img/grafana.png)

---

## 🔒 Garantias e decisões

| Garantia / decisão | Como é feita | Onde |
|---|---|---|
| **Bronze sem duplicatas** | Checkpoint do Structured Streaming + `MERGE` Delta por `event_id`: reprocessar offsets não duplica linhas (efeito exactly-once na Bronze). | [`ingestion_job.py`](spark_jobs/ingestion_job.py), [`delta_utils.py`](spark_jobs/utils/delta_utils.py) |
| **Produção idempotente** | `enable.idempotence=True` + `acks=all` + `retries=5`. | [`base_producer.py`](producers/base_producer.py) (`PRODUCER_CONFIG`) |
| **Estado do pedido exactly-once até o tópico** | Kafka Streams com `exactly_once_v2`: offsets, changelogs e saídas na mesma transação. | [`order-status-service`](#-order-status-service--kafka-streams-java) |
| **MongoDB sem transação distribuída** | Os dois caminhos entregam *at-least-once* e gravam por upsert de chave (`order_id`, `alert_id`, `scope:key`): repetir um registro ou um micro-batch dá o mesmo documento. | [Connect](#-camada-de-serviço--mongodb-via-kafka-connect), [Scala](#-delivery-sla-job--linha-do-tempo-de-entrega-e-sla-scala) |
| **Falha do MongoDB não perde dado** | O sink para (`mongo.errors.tolerance=none`) sem confirmar offsets e `connect-init` reinicia as tarefas; o job Scala falha o micro-batch, o contêiner reinicia e retoma do checkpoint. Testado parando o MongoDB por um minuto. | [Connect](#-camada-de-serviço--mongodb-via-kafka-connect) |
| **Registro ilegível não para o fluxo** | Kafka Streams loga e segue; o Connect manda à DLQ (`errors.tolerance=all`); o job Scala loga e descarta. | seções de cada componente |
| **Eventos fora de ordem e repetidos** | Watermark de 10 min nas janelas do Spark Python; estado comutativo por pedido no Kafka Streams e no job Scala (menor timestamp por marco), testado com todas as 120 ordens de chegada de 5 eventos. | seções de cada componente |
| **Particionamento** | Os produtores usam o CRC32 do librdkafka e os tópicos têm 3 e 2 partições: o Kafka Streams reparticiona, o job Scala agrupa por `order_id` (shuffle do Spark). Nenhum dos dois depende de co-particionamento. | seções de cada componente |
| **Volume por micro-batch limitado** | `maxOffsetsPerTrigger=10000` (Kafka) e `maxFilesPerTrigger=10` (Delta). | [`ingestion_job.py`](spark_jobs/ingestion_job.py), [`enrichment_job.py`](spark_jobs/enrichment_job.py) |
| **Segredos fora do git** | Senhas só no `.env` gerado; a URI do sink só como variável do worker (`EnvVarConfigProvider` com allowlist); usuários do MongoDB por papel (`connect`, `dashboard` só-leitura, `spark`). | [seção MongoDB](#-camada-de-serviço--mongodb-via-kafka-connect) |
| **Schema evolution** | Campos novos opcionais com `default` (compatível BACKWARD) e acrescentados no fim do record, porque o job Python lê com schema fixo; o job Scala lê com o schema do escritor. | [seção Scala](#-delivery-sla-job--linha-do-tempo-de-entrega-e-sla-scala) |

---

## ⚠️ Limitações

O que não foi feito ou não funciona como se esperaria de produção. Cada seção
abaixo detalha as suas.

- **Dados simulados no tempo.** Os três produtores percorrem os CSVs em ritmos
  independentes e carimbam os eventos na emissão: os horários entre fontes não são
  coerentes (entrega pode vir antes da criação) e um ciclo de pedido dura segundos.
  Por isso o SLA usa as datas de negócio da Olist, não os horários dos eventos.
- **Replay reusa `order_id`.** O Kafka Streams guarda o estado para sempre e o job
  Scala tira do estado pedidos parados há 1 h. Um pedido cancelado numa passada
  antiga continua `CANCELED` no caminho Kafka Streams e pode aparecer entregue no
  caminho Scala; os dois caminhos podem divergir sobre o mesmo pedido.
- **Um nó de cada coisa.** Kafka, Connect, MongoDB e Spark (`local[*]`) sem réplica:
  é um ambiente de demonstração, sem alta disponibilidade.
- **Memória.** A stack completa soma ~8 GiB. Na máquina de desenvolvimento
  (Windows, 32 GB, com outros apps abertos) o Docker Desktop caiu algumas vezes por
  memória comprometida no limite; a stack inteira de uma vez não foi medida.
- **Observabilidade parcial.** MongoDB, Kafka Connect e o job Scala não exportam
  métricas para o Prometheus/Grafana.
- **CI.** Roda lint e testes de Python, Java e Scala, constrói todas as imagens e o
  teste ponta a ponta dos dois caminhos até o MongoDB. O pipeline Python e os
  painéis não entram no teste ponta a ponta (só nos testes deles). As imagens Docker
  não usam cache entre execuções do CI.
- **Dataset real não testado nestas fases.** MongoDB, Connect e o job Scala foram
  verificados só com a amostra sintética; o dataset da Kaggle não foi baixado (sem
  credenciais da Kaggle no ambiente de desenvolvimento).
- **Dois "SLA" diferentes.** O `delivery_sla_tier` do enrichment Python (SP = D+3,
  RJ/MG/ES = D+5, demais = D+8) é uma regra fixa, não vem dos dados; o SLA do job
  Scala compara a entrega real com a data estimada da Olist.
- **Nuvem.** Nada roda fora do Docker local ainda (a fase com Azure é opcional e não
  foi feita).

---

## 📥 Como baixar o dataset Olist

Requer a [Kaggle API](https://github.com/Kaggle/kaggle-api) configurada (`~/.kaggle/kaggle.json`):

```bash
pip install kaggle   # no host, só para o download
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
```

Após o download, `data/raw/` deve conter, entre outros:
`olist_orders_dataset.csv`, `olist_order_items_dataset.csv`,
`olist_customers_dataset.csv`, `olist_order_payments_dataset.csv`,
`olist_products_dataset.csv`.

Com os CSVs em `data/raw/` antes do `docker compose up`, o `init` os usa e gera a
dimensão `data/reference/customers` (parquet) do join estático do enrichment.
Para trocar a amostra sintética pelos dados reais depois, rode
`docker compose down -v`, apague `data/` (exceto os CSVs novos) e suba de novo.

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

## ☕ `order-status-service` — Kafka Streams (Java)

**Papel no pipeline:** o Spark responde "quanto vendemos por janela"; este serviço
responde "em que pé está o pedido X agora?" e "que pedidos violam uma regra?". Ele
junta os três tópicos de entrada num estado atual por pedido, publica esse estado
em `order-status` (compactado) e gera alertas em `order-alerts`. É a fonte do
estado de pedido que a camada de serviço (MongoDB, seção seguinte) expõe.

```
orders-raw ──────┐                                          ┌─► order-status (compactado)
payments-raw ────┼─► repartition ─► cogroup ─► KTable ──────┤
delivery-events ─┘   (3 partições)            (order_id)    └─► regras ─► order-alerts
                                                 ▲
                              GET /orders/{id} ──┘  (interactive query, store order-state)
```

**Ciclo de vida** (`status`): `CREATED → PAID → SHIPPED → DELIVERED`, e
`CANCELED` como estado terminal.

| Marco | Evento que o define |
|---|---|
| `created_at` | `ORDER_CREATED` |
| `paid_at` | primeiro `PaymentEvent` do pedido (`ORDER_APPROVED` fica em `approved_at`) |
| `shipped_at` | `ORDER_SHIPPED` ou `PICKED_UP` da transportadora, o que vier antes |
| `delivered_at` | `ORDER_DELIVERED` ou `DELIVERED` da transportadora |
| `canceled_at` | `ORDER_CANCELED` |

**Regras de alerta** (definidas a partir dos campos que os eventos têm de fato):

| Alerta | Quando |
|---|---|
| `PAYMENT_TIMEOUT` | pedido criado, sem nenhum `PaymentEvent` e não cancelado `PAYMENT_TIMEOUT_MINUTES` (15) depois, em *stream time* |
| `DELIVERED_AFTER_CANCELLATION` | a transportadora reporta `DELIVERED` para um pedido cancelado, em qualquer ordem de chegada |

Cada alerta sai uma vez por pedido, com `alert_id = <tipo>:<order_id>`, então um
consumidor pode fazer upsert idempotente.

**Decisões técnicas**

| Decisão | Por quê |
|---|---|
| `cogroup` → uma `KTable` por `order_id` | Três fontes com tipos diferentes agregadas num único estado, sem join em cascata nem tipo intermediário. |
| Repartição das três entradas | `delivery-events` tem 2 partições e as outras 3; e os produtores Python usam o particionador do librdkafka (CRC32), não o murmur2 do Java. Repartir pelo particionador do Streams co-particiona as três e faz `queryMetadataForKey` achar a partição certa. Alternativa (não adotada, mudaria os produtores): `partitioner=murmur2_random` no librdkafka. |
| Agregação comutativa e idempotente | Cada marco guarda o menor timestamp; o último status da transportadora é o de maior timestamp; pagamentos são deduplicados por conteúdo (`tipo\|valor\|parcelas`), porque o loop de replay reenvia o mesmo pagamento com outro `event_id`. Resultado: eventos fora de ordem ou repetidos convergem para o mesmo estado (testado com as 120 ordens possíveis de 5 eventos). |
| Punctuator em **stream time** | O prazo de pagamento é medido pelo timestamp dos registros, não pelo relógio: reprocessar um backlog não dispara alertas falsos só porque o pagamento ainda está mais atrás no tópico. Pedidos sem pagamento ficam numa store própria (`awaiting-payment`), então o punctuator não varre a KTable inteira. |
| `processing.guarantee=exactly_once_v2` | Offsets consumidos, changelogs das stores e escritas em `order-status`/`order-alerts` na mesma transação. |
| Erro de desserialização: log e segue | Um registro corrompido não derruba o serviço; aparece na métrica `dropped-records` do Kafka Streams. |
| Serdes Avro da Confluent | Mesmo Schema Registry dos produtores; as classes de entrada são geradas dos mesmos `.avsc` de `producers/schemas` (sem cópia). |

**Como usar** (sobe junto com `docker compose up -d --build`):

```bash
curl http://localhost:8088/orders/order_000000   # estado atual (JSON)
curl http://localhost:8088/metrics               # Prometheus
curl http://localhost:8088/health
```

Exemplo real (saída do teste de fumaça do CI):

```json
{
  "order_id": "order_000000", "status": "DELIVERED", "customer_state": "PR",
  "order_value": 255.92, "paid_amount": 271.51, "payment_count": 1,
  "payment_types": ["DEBIT_CARD"],
  "created_at": "2026-10-06T17:20:26.502Z", "paid_at": "2026-10-06T17:21:09.491Z",
  "shipped_at": "2026-10-06T17:20:26.455Z", "delivered_at": "2026-10-06T17:20:26.786Z",
  "last_delivery_status": "DELIVERED", "event_count": 9
}
```

Com várias instâncias, `GET /orders/{id}` numa instância que não tem a partição
do pedido responde `307` apontando para a dona (via `application.server`).

**Testes:** `order-status-service/src/test` usa `TopologyTestDriver` com Schema
Registry em memória (`mock://`), sem broker: ciclo de vida, todas as permutações
de chegada, deduplicação, replay, as duas regras (casos positivos e negativos) e a
consulta à store. `scripts/smoke_e2e.sh` sobe a stack real e consulta o
serviço; roda no CI. Build e testes: `cd order-status-service && ./gradlew test`
(ou só pelo CI/Docker; não precisa de Java instalado para rodar o pipeline).

**Limitações conhecidas**

- Os três produtores percorrem os CSVs em ritmos independentes, então os tempos
  entre fontes não são coerentes: `PICKED_UP` pode vir antes de `ORDER_CREATED`, e o
  produtor de pagamentos soma até 5 min "no futuro" ao `event_timestamp`
  (`paid_at` posterior à entrega). O serviço reporta o que os eventos dizem.
- Na amostra sintética o pagamento sai do mesmo índice do pedido, então quase não
  há `PAYMENT_TIMEOUT`. Com o dataset da Kaggle, os pagamentos chegam em outra
  ordem e o alerta dispara muito (o que é o comportamento esperado da regra).
- Na amostra sintética `paid_amount` difere de `order_value` (o gerador sorteia o
  frete duas vezes); por isso não há regra de divergência de valor.
- O loop de replay reusa os `order_id`: o serviço trata a segunda passada como o
  mesmo pedido (marcos e valores não mudam; só `event_count`/`updated_at`).
- Deduplicar pagamento por conteúdo junta dois pagamentos idênticos do mesmo
  pedido (mesmo tipo, valor e parcelas); o evento não traz o número sequencial.

---

## 🍃 Camada de serviço — MongoDB via Kafka Connect

**Papel no pipeline:** guardar o estado atual de cada pedido e os alertas num
banco de consulta, para quem precisa ler "o pedido X" sem consumir Kafka nem
depender de qual instância do Kafka Streams tem a partição do pedido.

```
order-status ─┐                            ┌─► olist_serving.order_status  (1 doc por order_id)
              ├─► Kafka Connect ──────────┤
order-alerts ─┘   MongoDB Sink (3 tasks    └─► olist_serving.order_alerts  (1 doc por alert_id)
                  por conector, upsert)          │
        falha de conversão/escrita ─► order-status-dlq / order-alerts-dlq
                                                 ▼
                         Streamlit: página "Consulta de pedido" (usuário só-leitura)
```

| Peça | O que é |
|---|---|
| `mongo` | `mongo:7.0.43`, cache WiredTiger de 256 MB. `mongo/init/01-serving.js` roda no primeiro start do volume: cria os usuários e os índices. |
| `connect` | `connect/Dockerfile`: `cp-kafka-connect:7.5.0` (Kafka 3.5, Java 11) + `mongo-kafka-connect-3.1.2-all.jar` do Maven Central, com SHA-256 conferido no build. Modo distribuído (um worker), chave `StringConverter`, valor `AvroConverter` + Schema Registry. |
| `connect-init` | One-shot (`curlimages/curl`): `PUT /connectors/<nome>/config` para cada JSON de `connect/connectors/` (cria ou atualiza; rodar de novo não duplica) e espera conector e tarefas `RUNNING`. |
| Página `Consulta de pedido` | `dashboard/pages/1_Consulta_de_pedido.py` (a página original não mudou). Status, linha do tempo, pagamentos, alertas do pedido e os pedidos atualizados mais recentes por status. A lógica fica em `dashboard/order_lookup.py`, testada sem MongoDB. |

**Configuração do sink** (`connect/connectors/*.json`):

| Configuração | Por quê |
|---|---|
| `PartialValueStrategy` (`order_id` / `alert_id`) + `ReplaceOneBusinessKeyStrategy` | Upsert pela chave de negócio: o documento do pedido é substituído pelo estado mais novo. O `_id` continua um `ObjectId`; a unicidade é do índice único em `order_id` (`alert_id` nos alertas). |
| `errors.tolerance=all` + DLQ | Um registro que não converte (Avro ilegível) vai para `order-status-dlq` / `order-alerts-dlq` com o erro nos headers, e o conector segue. O teste ponta a ponta falha se uma DLQ ganhar registros durante a execução, então a tolerância não esconde erro. |
| `mongo.errors.tolerance=none` | Erro ao **gravar** no MongoDB (banco reiniciando, rede) para a tarefa em `FAILED` sem confirmar offsets, em vez de mandar o registro à DLQ. Um mongod sem réplica não tem retryable writes e o conector não tem retentativa própria; com `all`, uma queda do MongoDB mandou 3 atualizações reais à DLQ durante o desenvolvimento. Recuperação: `docker compose up connect-init` reinicia as tarefas `FAILED` e elas continuam do último offset confirmado (testado parando o MongoDB por um minuto). |
| `connection.uri=${env:MONGO_SINK_URI}` | A URI com senha só existe como variável de ambiente do worker, lida pelo `EnvVarConfigProvider`. O JSON e a API REST (`GET /connectors/.../config`) mostram só o placeholder, e `config.providers.env.param.allowlist.pattern=^MONGO_SINK_URI$` impede um conector de ler qualquer outra variável. |
| `tasks.max=3` | Uma tarefa por partição de `order-status`. Como o tópico é chaveado por `order_id`, todas as versões de um pedido passam pela mesma tarefa, em ordem: a última escrita é o estado mais novo. |

**Usuários (menor privilégio)**, criados em `admin`, com papel só em `olist_serving`:

| Usuário | Papel | Quem usa |
|---|---|---|
| `connect` | `readWrite` | Kafka Connect |
| `dashboard` | `read` | Streamlit (o teste de fumaça confirma que um `insert` dá `Unauthorized`) |
| `spark` | `readWrite` | Job Scala (fase 3) |

**Índices:** `order_status` com `{order_id: 1}` único e `{status: 1, updated_at: -1}`
(serve a lista de recentes por status; conferido com `explain()`); `order_alerts`
com `{alert_id: 1}` único e `{order_id: 1}`.

### Por que o microsserviço não grava direto no MongoDB

- **Separação de responsabilidades.** O `order-status-service` calcula o estado;
  ele não sabe quem consome. O tópico compactado `order-status` é o contrato: hoje
  o MongoDB lê dele, amanhã outro destino lê do mesmo tópico sem tocar no serviço.
- **Garantia de ponta a ponta sem transação distribuída.** O Kafka Streams é
  exactly-once até o tópico (`exactly_once_v2`). Gravar no MongoDB de dentro dele
  sairia da transação: um reprocessamento repetiria a escrita. Com o sink, a
  entrega no MongoDB é *at-least-once* e o upsert por `order_id` é idempotente,
  então repetir um registro dá o mesmo documento.
- **Reprocessamento sem código.** Para recriar a coleção, basta apagá-la e
  reiniciar o conector do início do tópico (ver abaixo). O tópico compactado
  guarda o último estado de cada pedido.
- **Retries e backpressure prontos.** O Connect controla offsets, tenta de novo
  quando o MongoDB fica indisponível, e o consumo segue o ritmo das escritas. Se
  o banco ficar lento, o Kafka Streams continua processando: o atraso fica no
  conector, visível no consumer group `connect-order-status-mongo-sink`.

### Como usar

```bash
curl -s 'http://localhost:8083/connectors?expand=status'      # conectores e tarefas
# Página: http://localhost:8501/Consulta_de_pedido

# Shell no MongoDB como usuário só-leitura (a senha fica dentro do contêiner):
docker compose exec mongo sh -c 'mongosh "mongodb://dashboard:$MONGO_DASHBOARD_PASSWORD@localhost:27017/olist_serving?authSource=admin"'
> db.order_status.findOne({order_id: "order_000000"}, {_id: 0})
```

Reprocessar `order_status` do zero (apaga os documentos e mantém os índices):

```bash
curl -X DELETE http://localhost:8083/connectors/order-status-mongo-sink
docker compose exec mongo sh -c 'mongosh --quiet -u root -p "$MONGO_INITDB_ROOT_PASSWORD" \
  --eval "db.getSiblingDB(\"olist_serving\").order_status.deleteMany({})"'
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --group connect-order-status-mongo-sink --reset-offsets --to-earliest \
  --topic order-status --execute
docker compose up connect-init          # registra o conector de novo
```

O reset de offsets só é aceito quando o group fica inativo, alguns segundos
depois do `DELETE`; se ele reclamar de membros ativos, repita.

Exemplo real (saída do teste de fumaça, documento de `order_000000` no MongoDB,
com o mesmo status que `GET /orders/order_000000` retornou):

```js
{
  order_id: 'order_000000', status: 'CANCELED', customer_state: 'DF',
  order_value: 224.81, paid_amount: 223.09, payment_count: 1, payment_types: [ 'BOLETO' ],
  created_at: ISODate('2026-10-08T17:58:05.092Z'), canceled_at: ISODate('2026-10-08T17:58:29.918Z'),
  delivered_at: ISODate('2026-10-08T17:58:05.148Z'), last_delivery_status: 'DELIVERED',
  event_count: Long('52'), ...
}
```

**Testes:** `tests/unit/test_order_lookup.py` (busca, linha do tempo, pagamentos,
alertas, filtro/ordem/limite) com uma coleção falsa; a página foi exercitada com
o `AppTest` do Streamlit contra o MongoDB real. `scripts/smoke_e2e.sh`
(no CI) sobe Kafka, produtores, serviço, MongoDB e Connect e confere:
conectores `RUNNING`, `order_000000` com o mesmo status na API e no MongoDB
(com nova tentativa, porque os produtores seguem rodando), alertas em
`order_alerts`, nenhum registro novo nas DLQs, tarefas ainda `RUNNING` no fim e o
usuário `dashboard` sem permissão de escrita.
As senhas do CI são geradas na hora por `scripts/gen_env.sh`.

**Limitações conhecidas**

- Os usuários e índices só são criados no primeiro start de um volume vazio
  (comportamento da imagem oficial). Trocar uma senha no `.env` depois disso
  exige `updateUser` ou `docker compose down -v` (que apaga os dados do MongoDB).
- Um único worker do Connect, sem réplica do MongoDB: é um ambiente de
  demonstração, sem alta disponibilidade.
- O MongoDB e o Connect ainda não aparecem no Prometheus/Grafana.
- Durante um reprocessamento a coleção passa por estados intermediários (as
  versões antigas do tópico, antes da compactação) até alcançar o fim do tópico.
- A linha do tempo da página mostra os marcos na ordem dos horários dos eventos,
  que nem sempre é a ordem do negócio (ver limitações do serviço acima).

---

## 🚚 `delivery-sla-job` — linha do tempo de entrega e SLA (Scala)

**Papel no pipeline:** o `order-status-service` diz em que pé está o pedido; este
job responde "a entrega cumpriu o prazo prometido?" e "qual o SLA por estado e
região?". Ele lê `orders-raw` e `delivery-events`, mantém uma linha do tempo de
entrega por pedido e compara a entrega real com a data estimada da Olist.

```
orders-raw ──────┐   decode Avro com o                        ┌─► Delta gold /data/gold/delivery_timeline (MERGE)
                 ├─► schema do escritor ─► groupByKey ─► flatMapGroupsWithState ─┤
delivery-events ─┘   (Schema Registry)    (order_id)   (estado por pedido)      ├─► olist_serving.delivery_timeline
                                                                                 └─► olist_serving.delivery_sla
                                                    (recalculado do gold a cada micro-batch: UF, região, Brasil)
```

**Por que Scala** (o resto do Spark do projeto é Python):

- **Estado arbitrário tipado.** `flatMapGroupsWithState` com estado próprio
  (`OrderDeliveryState`, uma case class) e timeout só existe nas APIs
  Scala/Java. No PySpark 3.4 o equivalente (`applyInPandasWithState`) recebe os
  eventos como DataFrames pandas e o estado como tupla com schema declarado em
  string: nada é checado antes de rodar, e cada grupo passa por Arrow.
- **Datasets tipados de ponta a ponta.** `Dataset[DeliveryInput]` →
  `Dataset[DeliveryTimeline]` → `Dataset[SlaSummary]`, com case classes: um
  campo errado ou um `Option` esquecido falha na compilação, não no meio do
  stream.
- **Lógica testável sem Spark.** A fusão de eventos é uma função pura sobre case
  classes (`OrderDeliveryState.add`), testada sem SparkSession.

**Decisões técnicas**

| Decisão | Por quê |
|---|---|
| Datas de negócio nos eventos de pedido | Os eventos são carimbados na emissão e um ciclo do replay dura segundos: medir atraso por eles não diria nada. `order_event.avsc` ganhou `purchase_ts`, `estimated_delivery_ts` e `delivered_customer_ts` (opcionais, `default: null`, compatível BACKWARD), vindos das colunas do CSV da Olist. |
| Campos novos **no fim** do record | O `ingestion_job` (Python) decodifica com um schema fixo no código, sem consultar o Registry; campos acrescentados no fim são ignorados por ele. Um teste de integração (`test_avro_reader_ignores_fields_appended_to_order_event`) garante isso. |
| Decodificação com o schema do escritor | O job lê o id do schema no header Confluent e busca o schema no Registry (com cache). Assim lê igual mensagens antigas (sem as datas → `None`) e novas. Não usei o `kafka-avro-serializer` da Confluent para não trazer Guava e kafka-clients em versões que brigam com o classpath do Spark. Registro ilegível é logado e descartado. |
| `groupByKey(order_id)` | O Spark faz shuffle por hash da chave, então os eventos de um pedido se encontram no mesmo estado qualquer que seja a partição Kafka de origem. Os produtores particionam por CRC32 (librdkafka) e os tópicos têm 3 e 2 partições: nada aqui depende de co-particionamento. |
| Estado comutativo | Marco = menor timestamp; último status do transportador = maior timestamp (empate vai para a etapa posterior); datas de negócio = menor valor. Chegada fora de ordem ou repetida converge para o mesmo estado (testado com as 120 ordens de 5 eventos). |
| Timeout por tempo de processamento (`STATE_TTL`, 1 h) | Pedido sem evento novo há 1 h sai do estado (a linha do tempo já foi gravada). O estado fica limitado aos pedidos ativos. |
| SLA no tempo de negócio | `LATE` quando a entrega ao cliente passa do dia estimado (a Olist dá uma data, 00:00); `delay_days` = entrega − data estimada (negativo = adiantado). `PENDING` sem entrega, `UNKNOWN` sem data estimada, `CANCELED` excluído do SLA. Regiões do IBGE. |
| `foreachBatch` com upserts | Cada micro-batch: MERGE no Delta gold por `order_id`, upsert no MongoDB (`_id = order_id`, MongoDB Spark Connector 10.4.1) e SLA recalculado de toda a tabela gold (`_id = scope:key`). Como tudo é upsert por chave, repetir um batch após falha dá o mesmo resultado. |

**Exemplo real** (`olist_serving.delivery_timeline`, amostra sintética):

```js
{
  _id: 'order_000170', order_id: 'order_000170', customer_state: 'MG', region: 'Sudeste',
  delivery_status: 'DELIVERED', sla_status: 'LATE',
  delay_days: 10.42, promised_days: 23.17, actual_days: 33.58,
  purchase_ts: ISODate('2024-08-27T20:00:00Z'), estimated_delivery_ts: ISODate('2024-09-20T00:00:00Z'),
  delivered_customer_ts: ISODate('2024-09-30T10:00:00Z'),
  created_at: ISODate('2026-10-08T17:59:36.378Z'), shipped_at: ..., in_transit_at: ...,
  out_for_delivery_at: ..., delivered_at: ..., last_carrier_status: 'DELIVERED', ...
}
```

`delivery_sla` tem um documento por UF (`state:SP`), região (`region:Sudeste`) e
o país (`all:BR`): `delivered_orders`, `on_time_orders`, `late_orders`,
`on_time_rate`, `avg_delay_days_when_late`, `avg_promised_days`,
`avg_actual_days`, `pending_orders`, `canceled_orders`.

**Como usar** (sobe junto com `docker compose up -d --build`; Spark UI em http://localhost:4041):

```bash
docker compose exec mongo sh -c 'mongosh "mongodb://dashboard:$MONGO_DASHBOARD_PASSWORD@localhost:27017/olist_serving?authSource=admin"'
> db.delivery_sla.find({scope: "region"}).sort({on_time_rate: 1})
> db.delivery_timeline.find({sla_status: "LATE"}).sort({delay_days: -1}).limit(5)
```

**Testes** (`delivery-sla-job/src/test`, ScalaTest, sem Kafka nem MongoDB; no CI):
lógica do estado (todas as ordens de chegada, duplicatas, cancelamento, SLA e
regiões), `MemoryStream` com 3 partições passando por `foreachBatch` (estado entre
micro-batches, só pedidos tocados saem), `TestGroupState` para o TTL, o sink contra
Delta com um escritor de documentos em memória (inclusive batch repetido) e a
decodificação Avro com os `.avsc` dos produtores (com e sem as datas, registro
corrompido). Rodar: `cd delivery-sla-job && sbt test`, ou via Docker:
`docker run --rm -v "${PWD}:/src" -w /src/delivery-sla-job sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.22_7_1.9.9_2.12.18 sbt test`.

**Limitações conhecidas**

- A amostra sintética gerada antes desta fase não tem as datas: os pedidos saem
  `UNKNOWN`. Para gerar de novo: `docker compose run --rm --no-deps init python scripts/seed_data.py --sample`
  e reinicie os produtores. Eventos antigos que ainda estão nos tópicos também saem sem datas.
- O replay sorteia o cancelamento (12%) a cada passada pelo CSV e reusa os
  `order_id`; como o estado guarda o cancelamento mais antigo, um pedido cancelado
  em qualquer passada fica `CANCELED` (na primeira medição, ~23% dos pedidos).
- Os marcos do transportador são horários de emissão de produtores independentes
  (`delivered_at` pode vir antes de `in_transit_at`); o SLA não usa esses horários.
- Os testes desligam `spark.sql.streaming.noDataMicroBatches.enabled`: com timeout
  por tempo de processamento cada trigger roda um batch sem dados e
  `processAllAvailable()` nunca veria o stream parado. O TTL é testado com
  `TestGroupState`; a expiração dentro de uma query rodando não tem teste automatizado.
- O SLA é recalculado lendo toda a tabela gold a cada micro-batch: simples e
  funcionou com os 10.000 pedidos da amostra; volumes maiores pediriam agregação
  incremental (não medido).
- Uma queda do MongoDB faz o micro-batch falhar e a query parar; quem a retoma é o
  `restart: unless-stopped` do contêiner, a partir do checkpoint (visto ao parar o
  MongoDB por um minuto). Não há retentativa dentro do job.

---

## Consumo de memória medido

`docker stats` em 2026-10-08, Docker Desktop (WSL2) no Windows, amostra sintética:

| Serviço | RAM |
|---|---|
| `spark-pipeline` | 2,5 GiB |
| `delivery-sla-job` (driver `1g`, `local[2]`) | 2,3 GiB |
| `connect` (heap `-Xmx512m`) | 0,9 GiB |
| `kafka` | 0,5–0,7 GiB |
| `schema-registry`, `kafka-ui` | ~0,35 GiB cada |
| `mongo` | 0,2–0,3 GiB |
| `order-status-service` | 0,24 GiB |
| demais (zookeeper, grafana, prometheus, dashboard, 3 produtores) | ~0,55 GiB |
| **Total** | **~8 GiB** (soma das medições) |

Os números foram medidos em rodadas diferentes (a máquina não aguentou tudo junto
com folga) e somados; a stack inteira de uma vez não foi medida. Dê ao Docker
**10 GB ou mais** para rodar tudo; sem o `spark-pipeline` e o `delivery-sla-job`
ficam ~3,2 GiB. O dashboard abre uma SparkSession própria quando alguém visita a
página principal; esse acréscimo não foi medido.

Na máquina usada para medir (32 GB, com navegador, IDEs e outros apps abertos),
o Docker Desktop caiu algumas vezes ao rodar a stack completa junto com testes
Spark em outro contêiner. Os logs mostram o backend encerrado sem erro próprio,
com a memória comprometida do Windows perto do limite. Se acontecer, feche
aplicativos ou limite a VM em `%UserProfile%\.wslconfig` (`[wsl2]` /
`memory=10GB`), e rode `wsl --shutdown` antes de reabrir o Docker Desktop.

---

## 📊 Monitoramento

Grafana provisiona automaticamente o datasource Prometheus e o dashboard
**"Olist Streaming Pipeline"** (`monitoring/grafana/dashboards/streaming_pipeline.json`):

| Painel | Métrica |
|---|---|
| Throughput | `rate(kafka_messages_produced_total{status="success"}[1m])` |
| Error rate | `rate(kafka_messages_produced_total{status="error"}[1m])` |
| Spark ingestion | `metrics_olist_driver_spark_streaming_ingestion_bronze_{inputRate,processingRate}_total_Value` |
| order-status-service: eventos/s | `sum by (source) (rate(order_status_input_events_total[1m]))` |
| order-status-service: alertas | `sum by (type) (increase(order_status_alerts_total[5m]))` |
| Total produzido | `sum(kafka_messages_produced_total{status="success"})` |

![Grafana](docs/img/grafana.png)

Até 2026-10-09 todos os painéis mostravam "No data": o dashboard procura o
datasource pelo uid `prometheus` e o provisionamento não definia uid. Corrigido em
`monitoring/grafana/provisioning/datasources/prometheus.yml`. MongoDB, Kafka
Connect e o job Scala ainda não têm métricas aqui.


---

## 🛠️ Troubleshooting

<details>
<summary><b>1. OOM no Spark (executor/driver morre)</b></summary>

O Spark roda em modo `local[*]` dentro do contêiner `spark-pipeline`.
Reduza o volume por micro-batch:
```python
# ingestion_job.py
.option("maxOffsetsPerTrigger", "1000")   # de 10000
```
Dê 10 GB ou mais ao Docker (ver [consumo medido](#consumo-de-memória-medido)).
</details>

<details>
<summary><b>2. Ingestão não acompanha a produção (backlog crescente)</b></summary>

O Spark guarda os offsets do Kafka no checkpoint e não os confirma num consumer
group, então não há "lag de grupo" para medir. Use o painel **Spark ingestion** do
Grafana: se `processed` fica abaixo de `input` por vários minutos, o backlog está
crescendo. Causas comuns: `EVENTS_PER_SECOND`
alto demais para a capacidade do Spark, ou trigger muito curto. Aumente paralelismo
(mais CPUs para o Docker; o `local[*]` usa todas), aumente `maxOffsetsPerTrigger` **com** mais memória, ou
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
query. Os checkpoints do pipeline Python ficam no volume `spark-checkpoints`
(`ingestion`, `enrichment`, `aggregation`, `aggregation_rate`). Limpe só o do job
afetado e reinicie:
```bash
# No Git Bash, MSYS_NO_PATHCONV=1 impede que /tmp vire um caminho do Windows.
MSYS_NO_PATHCONV=1 docker compose exec spark-pipeline rm -rf /tmp/streaming-checkpoints/ingestion
docker compose restart spark-pipeline
```
Como a Bronze usa `MERGE` por `event_id`, reprocessar do início **não** duplica dados.
O job Scala guarda o checkpoint no volume `delivery-sla-checkpoints`; como grava por
upsert, apagá-lo também só reprocessa.
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
docker compose ps schema-registry
```
Dentro do Docker a URL é `http://schema-registry:8081`; no host, `http://localhost:8081`.
(`make check` roda `scripts/check_pipeline.py` no host e precisa das dependências
Python instaladas.)
</details>

<details>
<summary><b>7. Conector do MongoDB com tarefas FAILED</b></summary>

Esperado depois de uma queda do MongoDB: o sink para em vez de descartar
registros. Com o MongoDB de volta:
```bash
docker compose up connect-init     # reinicia as tarefas FAILED e espera RUNNING
curl -s 'http://localhost:8083/connectors?expand=status'
```
As tarefas continuam do último offset confirmado. O erro aparece em
`docker compose logs connect`.
</details>

<details>
<summary><b>8. Docker Desktop cai no Windows com a stack completa</b></summary>

Visto durante o desenvolvimento quando a memória comprometida do Windows chegava ao
limite (a VM do WSL2 com ~8 GB e outros apps abertos). Feche aplicativos ou limite
a VM em `%UserProfile%\.wslconfig` (`[wsl2]` / `memory=10GB`), rode `wsl --shutdown`
e abra o Docker Desktop de novo. Se o Kafka não subir depois disso
(`NodeExistsException` no log), ele se recupera sozinho no próximo restart: o nó
efêmero antigo no ZooKeeper expira em segundos.
</details>

---

## 🧪 Testes

| O quê | Como | Onde roda |
|---|---|---|
| Python: produtores, jobs Spark, consultas do painel, consulta ao MongoDB | pytest com `SparkSession` local + Delta, sem Kafka (o teste marcado `RUN_KAFKA_IT=1` exige broker) | `make test` / CI |
| Python: lint e tipos | black, flake8, mypy | `make lint` / CI |
| Java: `order-status-service` | `TopologyTestDriver` + Schema Registry `mock://`, sem broker | `./gradlew test` / CI |
| Scala: `delivery-sla-job` | ScalaTest + `MemoryStream`, `TestGroupState`, Delta local, sem Kafka nem MongoDB | `sbt test` / CI |
| Ponta a ponta | `scripts/smoke_e2e.sh`: stack real, os dois caminhos até o MongoDB | local / CI |

Sem Java ou sbt instalados, os testes JVM rodam em contêiner:

```bash
docker run --rm -v "${PWD}:/src" -w /src/order-status-service eclipse-temurin:17-jdk ./gradlew test
docker run --rm -v "${PWD}:/src" -w /src/delivery-sla-job \
  sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.22_7_1.9.9_2.12.18 sbt test
```

O [CI](.github/workflows/ci.yml) roda em todo push: lint e testes Python (cache do
pip e do Ivy), testes Java (cache do Gradle), testes Scala (cache do sbt), build de
todas as imagens (e a checagem de que as imagens Spark Python usam Java 17) e o
teste ponta a ponta com senhas do MongoDB geradas na hora.

---

## 🗂️ Estrutura

```
producers/      # Avro producers (base + orders/payments/delivery) e schemas .avsc
spark_jobs/     # ingestion, enrichment, aggregation, runner, utils (kafka/delta)
dashboard/      # Streamlit: painel do Gold (Delta) e página de consulta no MongoDB
scripts/        # create_topics, register_schemas, seed_data, check_pipeline, gen_env.sh, up.sh, smoke_e2e.sh
order-status-service/  # Kafka Streams (Java 17, Gradle): estado por pedido + alertas
connect/        # imagem do Kafka Connect + MongoDB sink, configs dos conectores e registro
mongo/init/     # usuários de menor privilégio e índices do olist_serving
delivery-sla-job/  # Spark Structured Streaming em Scala (sbt): linha do tempo de entrega + SLA
monitoring/     # Prometheus + provisioning e dashboard Grafana
tests/          # testes Python: unit, integration, e2e
docs/img/       # capturas usadas neste README
.github/workflows/ci.yml  # CI: Python, Java, Scala, imagens e teste ponta a ponta
```

---

## 📄 Licença

Projeto educacional. Dataset Olist sob licença CC BY-NC-SA 4.0 (Kaggle).
