# LinkedIn Post — Kafka CDC Pipeline Architecture (Funny & Engaging)

---

## FULL ARTICLE VERSION (copy-paste ready)

---

😭 My data pipeline used to run on a cron job every 5 minutes.

For 4 minutes and 59 seconds, PostgreSQL had absolutely no idea what MySQL was doing.
Blissfully ignorant. Like me before I discovered Kafka.

And deleted rows? Completely invisible.
`updated_at` doesn't get updated when you DELETE a row.
Found that out in production. At 11pm. On a Friday.

So I rebuilt everything from scratch with Apache Kafka + Debezium CDC.

Here's the full architecture 👇

---

🏗️ **THE FULL ARCHITECTURE**

```
┌─────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                          │
│  ┌────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐ ┌───────┐ │
│  │ MySQL  │ │PostgreSQL│ │ Oracle │ │SQL Server│ │  CSV  │ │
│  └───┬────┘ └────┬─────┘ └───┬────┘ └────┬─────┘ └───┬───┘ │
└──────┼───────────┼───────────┼────────────┼───────────┼─────┘
       │           │           │            │           │
       ▼           ▼           ▼            ▼           ▼
┌─────────────────────────────────────────────────────────────┐
│                   INGESTION LAYER                           │
│         Kafka Connect (Source) + Debezium                   │
│  ┌─────────────┐ ┌─────────────┐ ┌────────────────────────┐ │
│  │ Debezium    │ │ Debezium    │ │   Kafka Connect        │ │
│  │ MySQL CDC   │ │ PG / Oracle │ │   FileStream / S3      │ │
│  └─────────────┘ └─────────────┘ └────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA CLUSTER                            │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │ Broker 1  │  │ Broker 2  │  │ Broker 3  │  (KRaft)      │
│  └───────────┘  └───────────┘  └───────────┘               │
│                                                             │
│  Topics: prod.mysql.sourcedb.orders                        │
│          prod.mysql.sourcedb.customers                     │
│          prod.dlq.errors                                   │
│                                                             │
│  ┌──────────────────────────────┐                          │
│  │       Schema Registry        │ ← data contracts         │
│  └──────────────────────────────┘                          │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              PROCESSING LAYER  (Optional)                   │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐ │
│  │Kafka Streams │ │    ksqlDB    │ │  Apache Flink /      │ │
│  │(lightweight) │ │ (SQL-based)  │ │  Spark Streaming     │ │
│  └──────────────┘ └──────────────┘ └──────────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     SINK LAYER                              │
│         Kafka Connect (Sink) — JDBC Sink Connector          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────┐ │
│  │JDBC Sink │ │JDBC Sink │ │JDBC Sink │ │ Custom Consumer│ │
│  │PostgreSQL│ │  MySQL   │ │SQL Server│ │ / Kafka Streams│ │
│  └──────────┘ └──────────┘ └──────────┘ └────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    DATA TARGETS                             │
│ ┌──────────┐ ┌────────┐ ┌──────────┐ ┌────────┐ ┌────────┐ │
│ │PostgreSQL│ │ MySQL  │ │SQL Server│ │ Oracle │ │  S3 /  │ │
│ │          │ │        │ │          │ │        │ │  Lake  │ │
│ └──────────┘ └────────┘ └──────────┘ └────────┘ └────────┘ │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│            OBSERVABILITY & CONTROL PLANE                    │
│  Prometheus + Grafana  │  Kafka UI  │  Kafka Connect API    │
│  Schema Registry UI    │  Alertmanager  │  Log aggregation  │
└─────────────────────────────────────────────────────────────┘
```

Yes I drew this by hand. Yes I'm proud of it. No I'm not okay.

---

🔍 **WHAT EACH LAYER ACTUALLY DOES**

**Layer 1 — Data Sources**
MySQL, PostgreSQL, Oracle, SQL Server, CSV files.
The databases that hold your precious data and absolutely do not want you touching their logs.
Spoiler: Debezium touches their logs anyway.

**Layer 2 — Ingestion (Kafka Connect + Debezium)**
Debezium reads the database's transaction log — the secret diary every database keeps about every change it ever made.

MySQL calls it the **binlog**.
PostgreSQL calls it the **WAL**.
Oracle calls it the **LogMiner** — and also calls your finance team for a license.

Every change becomes a Kafka message that looks like this:

```json
{
  "before": { "id": 1, "status": "PENDING"    },
  "after":  { "id": 1, "status": "PROCESSING" },
  "op":     "u",
  "ts_ms":  1700000000000,
  "source": { "db": "sourcedb", "table": "orders" }
}
```

`op` values:
→ `c` = INSERT (created)
→ `u` = UPDATE
→ `d` = DELETE ← the one my cron job never saw
→ `r` = initial snapshot read

Zero polling. Zero extra load on the source database.
Debezium just reads the log. Like a very fast, very reliable spy.

**Layer 3 — Kafka Cluster (KRaft mode)**
The central nervous system. All data flows through here.

Topics follow a naming convention:
```
prod.mysql.sourcedb.orders      ← environment.db.schema.table
prod.mysql.sourcedb.customers
prod.dlq.errors                 ← Dead Letter Queue (bad messages go here to think about what they did)
```

Each topic has partitions for parallelism, replication for fault tolerance, and a Schema Registry subject (`topic-value`) to enforce data contracts.

No Zookeeper in sight. KRaft mode handles cluster metadata directly.
RIP Zookeeper. Gone but not missed.

**Layer 4 — Processing (Optional but powerful)**

| Tool | Best For | Vibe |
|------|----------|------|
| Kafka Streams | Lightweight, in-app Java/Kotlin | "I just need a filter and a join" |
| ksqlDB | SQL over streams, ad-hoc queries | "I want streams but I only know SQL" |
| Apache Flink | Complex stateful, exactly-once, large scale | "I have a PhD and infinite patience" |
| Spark Streaming | Micro-batch + existing Spark ecosystem | "We already have Spark, don't judge us" |

**Layer 5 — Sink (Kafka Connect JDBC Sink)**
Reads from Kafka topics and upserts into the target database.
No custom consumer code needed for standard relational targets.
Mode: UPSERT. Primary key: `id`. Schema evolution: automatic.
It just works. Until it doesn't. See lessons below.

---

🔁 **5 DATA FLOW PATTERNS THIS UNLOCKS**

**Pattern 1: CDC Replication**
```
MySQL binlog → Debezium → Kafka → JDBC Sink → PostgreSQL
```
Real-time. Sub-500ms latency. This is why we're here.

**Pattern 2: File Ingestion**
```
CSV file → FileStream Connector → Kafka → JDBC Sink → DB
```
For the legacy system that still exports CSVs at midnight via FTP.
We don't judge. We just ingest.

**Pattern 3: Cross-DB Migration**
```
Oracle LogMiner → Debezium → Kafka → JDBC Sink → SQL Server
```
Migrating databases while they're still running.
Completely fine. Totally not terrifying. Everything is fine.

**Pattern 4: Fan-Out (One source → Multiple targets)**
```
MySQL binlog → Debezium → Kafka topic ─┬→ JDBC Sink → PostgreSQL
                                        ├→ JDBC Sink → MySQL replica
                                        └→ S3 Sink   → Data Lake
```
Write once. Sync everywhere.
Like a group chat — but the messages actually arrive.

**Pattern 5: Aggregation / Enrichment**
```
orders.cdc ─┐
             ├→ Kafka Streams / Flink → enriched.orders → Data Warehouse
users.cdc  ─┘
```
Join two CDC streams to enrich events in real time.
Because raw data is fine, but context is better.

---

📐 **DEPLOYMENT TOPOLOGY**

**Development (your laptop):**
```
┌──────────────────────────────────────┐
│  Docker Compose                      │
│  - 1x Kafka (KRaft, no Zookeeper)   │
│  - 1x Schema Registry               │
│  - 1x Kafka Connect + Debezium      │
│  - 1x Kafka UI                      │
│  - Prometheus + Grafana             │
└──────────────────────────────────────┘
Runs on 8 GB RAM. Works great.
(Until you open 12 Chrome tabs. Then it's a negotiation.)
```

**Production (Kubernetes):**
```
┌──────────────────────────────────────┐
│  Kubernetes                          │
│  - 3x KRaft Controllers             │
│  - 3x Kafka Brokers (StatefulSet)   │
│  - 2x Schema Registry (HA)          │
│  - 3x Kafka Connect Workers         │
│  - Prometheus + Grafana + Alerts    │
└──────────────────────────────────────┘
Replication factor: 3. Min ISR: 2.
Oncall rotation: yes. Sleep: negotiable.
```

---

💀 **5 LESSONS LEARNED THE PAINFUL WAY**

**1. `curl` in PowerShell is a liar.**
You type: `curl http://localhost:8083`
PowerShell gives you: three security warnings and a wall of HTML.
That's `Invoke-WebRequest` in disguise — not real curl.
The real curl is `curl.exe`. Four extra characters. Several hours of my life. You're welcome.

**2. `schemas.enable: false` is the silent destroyer.**
The sink connector started up. Said `RUNNING`. Looked healthy.
Wrote exactly **zero rows**. No error. No warning. Just vibes.
`schemas.enable: false` means "I have no idea what shape your data is, so I'll do nothing."
Set it to `true`. Both key AND value converter. Non-negotiable.

**3. Never pre-create your sink tables.**
I thought: "I'll be helpful! I'll create the PostgreSQL tables in advance!"
Debezium ran `ALTER TABLE` to add its CDC columns. Got: `ERROR: must be owner of table orders`.
Dropped the tables. Let Debezium auto-create them. Problem solved.
Lesson: Debezium knows what it's doing. I did not.

**4. Topics must exist BEFORE you register connectors.**
Kafka Connect stores its entire state in Kafka topics:
`_connect-configs`, `_connect-offsets`, `_connect-status`
If those don't exist? Connector registers successfully. Then fails silently.
Create the topics first. Always. Yes, even those internal ones.

**5. `kafka-exporter` keeps restarting?**
It just started before Kafka finished booting.
`docker compose restart kafka-exporter` — fixed in 3 seconds.
I Googled for 40 minutes before trying that.

---

🛠️ **FULL STACK (all open-source)**

- Apache Kafka `7.6.1` — KRaft mode
- Debezium Connect `2.6` — CDC engine
- Confluent Schema Registry `7.6.1` — data contracts
- Kafka UI (provectuslabs) — visibility
- Kafka Exporter — consumer lag metrics
- Prometheus + Grafana — the anxiety dashboard wall
- MySQL `8.0` → PostgreSQL `16`
- Python `3.13` (confluent-kafka, psycopg2, mysql-connector-python)

Total cost: $0.
Total hours debugging: priceless.

---

If your analytics team has ever asked "why is this row missing?" and the answer was "oh, someone deleted it 3 cron-job cycles ago and we never captured it" — this post is for you.

Drop a 🔥 if CDC just saved your architecture.
Drop a 😭 if you're still running cron jobs and this hit too close to home.
Drop a 🤔 if you have questions — happy to discuss CDC patterns, Debezium configs, or the existential horror of `schemas.enable: false`.

---

#Kafka #DataEngineering #CDC #RealTimeData #ApacheKafka #Debezium #DataPipeline
#StreamProcessing #PostgreSQL #MySQL #KafkaConnect #OpenSource #BackendEngineering
#DataArchitecture #Python #SoftwareEngineering #TechHumor #LearnedTheHardWay

---
---

## VERSION 2 — SHORT LINKEDIN ARTICLE (copy-paste ready)

How to post:
  1. Go to LinkedIn → Click "Write article" (not "Start a post")
  2. Paste the TITLE below into the title field at the top
  3. Paste the BODY below into the article body
  4. Add a cover image (architecture diagram screenshot recommended)
  5. Hit Publish

════════════════════════════════════════════════════════════
TITLE  ←  paste this into the LinkedIn Article title field
════════════════════════════════════════════════════════════

My Data Pipeline Ran on Cron Jobs. A Friday Night DELETE Broke Everything. So I Built This.

════════════════════════════════════════════════════════════
BODY  ←  paste everything below into the article body
════════════════════════════════════════════════════════════

😭 My old data pipeline ran on a cron job every 5 minutes.

For 4 minutes and 59 seconds, PostgreSQL had absolutely no idea what MySQL was doing. Blissfully ignorant.

And deleted rows? Completely invisible. `updated_at` doesn't get updated when you DELETE a row.

I found that out in production. On a Friday. At 11pm. In front of my team.

So I threw out the cron job and rebuilt everything with Apache Kafka + Debezium CDC. Here's the full architecture and everything I learned the hard way.


THE ARCHITECTURE

Every change in MySQL — every INSERT, UPDATE, and DELETE — now becomes a Kafka event in under 500ms. No polling. No missed rows. No surprises.

The 5-layer flow:

DATA SOURCES
  MySQL  |  PostgreSQL  |  Oracle  |  SQL Server  |  CSV Files
         |
         ↓
INGESTION — Kafka Connect + Debezium
  Debezium reads the database transaction log directly.
  MySQL calls it the binlog. PostgreSQL calls it the WAL.
  Oracle calls it LogMiner — and also calls your finance team.
  Every change becomes a message like this:

  {
    "before": { "id": 1, "status": "PENDING"    },
    "after":  { "id": 1, "status": "PROCESSING" },
    "op":     "u"
  }

  op: "c" = insert  |  "u" = update  |  "d" = delete  |  "r" = snapshot
  That "d" is the one my cron job never saw.
         |
         ↓
KAFKA CLUSTER — 3 Brokers + Schema Registry (KRaft mode, no Zookeeper)
  Topics follow a naming convention:
    prod.mysql.sourcedb.orders
    prod.mysql.sourcedb.customers
    prod.dlq.errors   ← where bad messages go to think about what they did
         |
         ↓
PROCESSING — Optional (Kafka Streams / ksqlDB / Apache Flink)
  Join streams, enrich events, aggregate in real time.
  Skip this layer entirely if you just need straight replication.
         |
         ↓
SINK — Kafka Connect JDBC Sink
  Reads from Kafka topics. Upserts into the target database.
  No custom consumer code needed for standard relational targets.
         |
         ↓
DATA TARGETS
  PostgreSQL  |  MySQL  |  SQL Server  |  Oracle  |  S3 / Data Lake


5 DATA FLOW PATTERNS THIS UNLOCKS

1. CDC Replication
   MySQL binlog → Debezium → Kafka → JDBC Sink → PostgreSQL
   Real-time. Sub-500ms. This is the whole point.

2. File Ingestion
   CSV file → FileStream Connector → Kafka → JDBC Sink → DB
   For the legacy system that still exports CSVs at midnight via FTP.
   We don't judge. We just ingest.

3. Cross-DB Migration
   Oracle LogMiner → Debezium → Kafka → SQL Server
   Migrating databases while they're still running. Totally fine. Not terrifying at all.

4. Fan-Out (One source → Multiple targets)
   MySQL → Kafka ──→ PostgreSQL
                ──→ MySQL replica
                ──→ S3 Data Lake
   Write once, sync everywhere. Like a group chat where messages actually arrive.

5. Enrichment
   orders.cdc  ──┐
                  ├→ Kafka Streams join → enriched.orders → Data Warehouse
   users.cdc   ──┘


DEPLOYMENT

On your laptop (Docker Compose, 8 GB RAM):
  1x Kafka (KRaft)  |  1x Schema Registry  |  1x Kafka Connect
  1x Kafka UI  |  Prometheus + Grafana
  Works great. Until you open 12 Chrome tabs. Then it's a negotiation.

In production (Kubernetes):
  3x KRaft Controllers  |  3x Kafka Brokers (StatefulSet)
  2x Schema Registry (HA)  |  3x Kafka Connect Workers
  Prometheus + Grafana + Alertmanager
  Oncall rotation: yes. Sleep: negotiable.


5 THINGS I LEARNED THE PAINFUL WAY

1. `curl` in PowerShell is a liar.
You type `curl http://localhost:8083` and PowerShell gives you three security warnings and a wall of HTML. That's `Invoke-WebRequest` wearing a disguise. The real curl binary is `curl.exe`. Four extra characters. Several hours of my life. You're welcome.

2. `schemas.enable: false` is the silent destroyer.
The sink connector said RUNNING. Looked healthy. Wrote exactly zero rows. No error. No warning. Just vibes. Setting `schemas.enable: false` means "I have no idea what shape your data is, so I'll do nothing." Set it to `true` on both key AND value converter. Non-negotiable.

3. Never pre-create your sink tables.
I thought I'd be helpful and create the PostgreSQL tables in advance. Debezium tried to ALTER TABLE to add its CDC columns and got: ERROR: must be owner of table orders. I dropped the tables, let Debezium auto-create them, and it worked immediately. Lesson: Debezium knows what it's doing. I did not.

4. Create Kafka's internal topics before registering connectors.
Kafka Connect stores its entire state inside Kafka topics: `_connect-configs`, `_connect-offsets`, `_connect-status`. If those don't exist yet, the connector registers successfully and then fails silently. Create all topics first. Always.

5. `kafka-exporter` keeps restarting?
It just started 10 seconds before Kafka finished booting. `docker compose restart kafka-exporter` fixed it in 3 seconds. I Googled for 40 minutes before trying that.


FULL STACK (all open-source, total cost: $0)

Apache Kafka 7.6.1 — KRaft mode (RIP Zookeeper, gone but not missed)
Debezium Connect 2.6 — CDC engine
Confluent Schema Registry 7.6.1 — data contracts
Kafka UI — so you can actually see what's happening
Prometheus + Grafana — the wall of anxiety dashboards
MySQL 8.0 → PostgreSQL 16
Python 3.13 — confluent-kafka, psycopg2, mysql-connector-python

Total debugging hours: priceless.


If your analytics team has ever asked "why is this row missing?" and the answer was "someone deleted it 3 cron-job cycles ago and we never captured it" — this article is for you.

Drop a 🔥 if CDC saved your architecture.
Drop a 😭 if cron jobs still haunt you.
Drop a 🤔 if you have questions on CDC patterns, Debezium configs, or Kafka Connect setup.

#Kafka #DataEngineering #CDC #RealTimeData #ApacheKafka #Debezium #DataPipeline #StreamProcessing #PostgreSQL #MySQL #KafkaConnect #OpenSource #BackendEngineering #DataArchitecture #Python #LearnedTheHardWay

════════════════════════════════════════════════════════════
END OF ARTICLE BODY
════════════════════════════════════════════════════════════


---
---

## FORMATTING NOTES

- LinkedIn Articles support bold and headers — but plain text pastes cleanly too.
- The architecture flow diagram pastes as readable plain text in the article body.
- Cover image: export the 5-layer architecture diagram as a PNG (draw.io, Excalidraw, or Canva).
- Best posting time: Tuesday–Thursday, 8–10am local time.
- After publishing the article, share it as a regular post with a 2-line teaser to drive traffic.
