# ⚡ Real-time Streaming Pipeline — Olist

[![Python](https://img.shields.io/badge/python-3.11-blue)](#)
[![Confluent Platform](https://img.shields.io/badge/Confluent%20Platform-7.5%20(Kafka%203.5)-231F20?logo=apachekafka)](#)
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

   PROMETHEUS  ◄── métricas (produtores + Spark streaming) ──►  GRAFANA
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

## 🚀 Como rodar

Só precisa de Docker (com ≥ 8 GB de RAM; a stack completa usa ~5,7 GiB, ver
[consumo medido](#consumo-de-memória-medido)) e Git
(no Windows, rode os scripts `.sh` pelo Git Bash); funciona igual no Windows,
macOS e Linux.

```bash
git clone <seu-fork> && cd streaming-mongodb-scala
scripts/gen_env.sh          # só na primeira vez: senhas do MongoDB no .env
docker compose up -d --build
```

O MongoDB exige senhas, e elas não ficam no repositório: `scripts/gen_env.sh`
cria (ou completa) o `.env`, ignorado pelo git, com senhas aleatórias e nunca as
imprime. Sem elas o compose para com
`required variable MONGO_ROOT_PASSWORD is missing a value: missing in .env, run scripts/gen_env.sh`.
Por isso, no primeiro uso, são dois comandos e não um.

O serviço `init` roda uma vez antes de produtores e pipeline: cria os tópicos,
registra os schemas Avro e prepara os dados. Se `data/raw/` tiver os CSVs reais
da Olist (ver seção abaixo), eles são usados; senão, gera uma amostra sintética.
O dashboard fica em http://localhost:8501 assim que o Gold tiver dados.

Com `make` disponível, `make up-all` gera o `.env` se faltar e sobe tudo;
`make help` lista os atalhos.

> **Windows, clone feito antes do `.gitattributes`:** se o build falhar com
> `./gradlew: not found`, os arquivos foram extraídos com CRLF (padrão do Git for
> Windows). Com a árvore limpa, rode `git rm -r --cached . && git reset --hard`
> para extraí-los de novo com LF.

> Dica para gravação de vídeo (LinkedIn): o produtor já roda com `PRODUCER_LOOP=true`,
> reiniciando o CSV ao chegar no fim — os contadores sobem continuamente.

### Portas

| Serviço | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |
| Spark UI (driver do pipeline) | http://localhost:4040 |
| Grafana | http://localhost:3000 (`admin` / `admin`) |
| Prometheus | http://localhost:9090 |
| Schema Registry | http://localhost:8081 |
| order-status-service (Kafka Streams) | http://localhost:8088/orders/{order_id} |
| Consulta de pedido (MongoDB) | http://localhost:8501/Consulta_de_pedido |
| Kafka Connect (REST) | http://localhost:8083/connectors?expand=status |
| MongoDB | `mongodb://localhost:27018` (só em 127.0.0.1; porta em `MONGO_HOST_PORT`) |

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
consulta à store. `scripts/smoke_order_status.sh` sobe a stack real e consulta o
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
| `errors.tolerance=all` + DLQ | Um registro que não converte ou não grava vai para `order-status-dlq` / `order-alerts-dlq` com o erro nos headers, e o conector segue. O teste de fumaça falha se alguma DLQ tiver registros, então a tolerância não esconde erro. |
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
o `AppTest` do Streamlit contra o MongoDB real. `scripts/smoke_order_status.sh`
(no CI) sobe Kafka, produtores, serviço, MongoDB e Connect e confere:
conectores `RUNNING`, `order_000000` com o mesmo status na API e no MongoDB
(com nova tentativa, porque os produtores seguem rodando), alertas em
`order_alerts`, DLQs vazias e o usuário `dashboard` sem permissão de escrita.
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

### Consumo de memória medido

`docker stats` em 2026-10-08, Docker Desktop (WSL2) no Windows, amostra sintética:

| Serviço | RAM |
|---|---|
| `spark-pipeline` | 2,5 GiB |
| `connect` (heap `-Xmx512m`) | 0,9 GiB |
| `kafka` | 0,5–0,7 GiB |
| `schema-registry`, `kafka-ui` | ~0,35 GiB cada |
| `mongo` | 0,3 GiB |
| `order-status-service` | 0,24 GiB |
| demais (zookeeper, grafana, prometheus, dashboard, 3 produtores) | ~0,55 GiB |
| **Total** | **~5,7 GiB** (3,1 GiB sem o `spark-pipeline`) |

O dashboard abre uma SparkSession própria quando alguém visita a página
principal; esse acréscimo não foi medido. A Fase 2 somou ~1,2 GiB (MongoDB +
Connect) aos ~4,7 GiB da stack anterior.

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

```
┌───────────────────────────┐  ┌───────────────────────────┐
│  Throughput (msgs/s)       │  │  Error rate (msgs/s)       │
│  [screenshot placeholder]  │  │  [screenshot placeholder]  │
└───────────────────────────┘  └───────────────────────────┘
┌───────────────────────────┐  ┌───────────────────────────┐
│  Spark ingestion (rows/s)  │  │  Total messages produced   │
│  [screenshot placeholder]  │  │  [screenshot placeholder]  │
└───────────────────────────┘  └───────────────────────────┘
```

> Substitua os placeholders por capturas reais de `http://localhost:3000` após `make up-all`.

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
Garanta ≥ 8 GB (idealmente 12 GB) disponíveis ao Docker.
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
scripts/        # create_topics, register_schemas, seed_data, check_pipeline, smoke_order_status.sh, gen_env.sh
order-status-service/  # Kafka Streams (Java 17, Gradle): estado por pedido + alertas
connect/        # imagem do Kafka Connect + MongoDB sink, configs dos conectores e registro
mongo/init/     # usuários de menor privilégio e índices do olist_serving
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
> (throughput e error rate), taxas de entrada/processamento do Spark no Grafana, e um
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
