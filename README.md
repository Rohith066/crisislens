# CrisisLens

**A real-time disaster-awareness platform.** CrisisLens streams authoritative hazard
feeds — earthquakes, severe weather, and more — through a durable Kafka pipeline into a
Delta lakehouse, and (in progress) answers *"what's happening near me, and what should I
do?"* with grounded, cited guidance.

All data comes from free, public government APIs (USGS, NOAA/NWS, NASA) — no proprietary
or private data is used.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-streaming-231F20?logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-lakehouse-00ADD4)

---

## The problem

When disaster strikes, life-saving information is **scattered** across a dozen government
sites and **slow** to reach people. CrisisLens unifies authoritative sources in real time
and makes them **askable in plain language** — for affected residents deciding whether to
evacuate, first responders needing multi-hazard situational awareness, and journalists who
want a single queryable feed of what's happening now.

## Architecture

```
  data sources          INGEST              PROCESS                SERVE                 OPERATE
  (public APIs)    →     Kafka          →   Spark Streaming   →    FastAPI + RAG    →    Terraform + AWS
                        producers           → Delta lakehouse       geo-aware agent       CI/CD + monitoring

  USGS quakes  ─┐
  NWS alerts   ─┼─▶ normalize to ──▶ topics ──▶ bronze ──▶ silver ──▶ gold ──▶ /hazards  + /ask
  (more soon)  ─┘   canonical schema          (raw)     (typed)    (agg)     geo feed   cited answers
```

Every source is normalized to **one canonical event schema** at ingestion, so every
downstream layer is source-agnostic:

```json
{
  "event_id":    "us7000...",
  "source":      "usgs",
  "hazard_type": "earthquake",
  "title":       "M 4.6 - 12km NE of Ridgecrest, CA",
  "severity":    4.6,
  "lat": 35.6, "lon": -117.6,
  "place":       "Ridgecrest, CA",
  "occurred_at": "2026-05-18T14:03:00Z",
  "updated_at":  "2026-05-18T14:05:00Z",
  "ingested_at": "2026-05-18T14:03:31Z",
  "url":         "https://earthquake.usgs.gov/...",
  "description": "..."
}
```

## Current status

| Phase | Scope | Status |
|---|---|---|
| **1 — Streaming ingestion** | Kafka producers (USGS + NWS/NOAA) → canonical schema, source-aware dedup, durable & restart-safe | ✅ **Complete** |
| **2 — Processing + lakehouse** | Spark Structured Streaming → Delta medallion (bronze → silver → gold), geo-enrichment, dbt | 🚧 In progress |
| **3 — RAG + agent serving** | FastAPI `/hazards` geo feed + `/ask` geo-aware RAG (local embeddings + FAISS + Ollama), cited & guardrailed; map UI next | 🚧 In progress |
| **4 — Deploy + operate** | Dockerize, Terraform on AWS, GitHub Actions CI/CD, pipeline + LLM monitoring | Planned |

**Phase 1 highlights:** two producers ingest live government feeds into Kafka through one
canonical schema, each with source-appropriate deduplication — USGS revises events *in
place* (dedup by `(event_id, updated)`), while NWS issues a *new id* per update (dedup by
`id`). Producers use `acks=all` + idempotent delivery and persist dedup state to disk, so
restarts resume cleanly without republishing. Verified with **zero duplicate event_ids**
across restarts on a live, high-churn feed.

## Tech stack

| Layer | Tech |
|---|---|
| Ingestion | Python, `confluent-kafka`, Apache Kafka (Docker) |
| Processing | Apache Spark Structured Streaming, Delta Lake (medallion), dbt |
| Serving | FastAPI, delta-rs, sentence-transformers + FAISS, Ollama (local LLM); Leaflet map (next) |
| Infra *(planned)* | Docker, Terraform, AWS, GitHub Actions CI/CD, monitoring |

## Quickstart (local)

**Prerequisites:** Docker, Python 3.12, and Java 17 (Spark runs on the JVM).

```bash
# 1. Start a local Kafka broker
docker compose up -d

# 2. Python environment
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt

# 3. Create the topics
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic earthquakes \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic weather-alerts \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 4. Run the producers (each in its own terminal)
./.venv/bin/python producers/usgs_producer.py
./.venv/bin/python producers/nws_producer.py

# 5. Land the streams into the Delta lakehouse (bronze)
./.venv/bin/python processing/bronze_stream.py
```

## Repository layout

```
producers/
  usgs_producer.py     # USGS earthquakes  -> topic `earthquakes`
  nws_producer.py      # NWS/NOAA alerts   -> topic `weather-alerts`
processing/
  bronze_stream.py     # Spark Structured Streaming: Kafka -> Delta bronze (raw)
  silver_stream.py     # bronze -> typed, deduped, cleaned silver (Delta MERGE)
  gold.py              # silver -> serving marts (map-ready hazards + rollups)
serving/
  api.py               # FastAPI: /hazards geo feed + /ask geo-aware RAG
  retrieval.py         # geo filter + FAISS semantic retrieval (local embeddings)
  rag.py               # grounded, cited answer via local Ollama LLM
docker-compose.yml     # local single-broker Kafka
requirements.txt
```

## What makes it different

1. **Geo-aware RAG** *(planned)* — retrieval filtered by distance from the user, combining
   spatial and semantic search, not just embedding similarity.
2. **Severity triage** *(planned)* — ranks events by real impact (magnitude × population ×
   infrastructure), not raw magnitude.
3. **Safety-critical guardrails** *(planned)* — every answer is grounded and cited; the agent
   refuses to guess and defers to local authorities when uncertain.

## Data sources & ethics

All feeds are free, public government infrastructure (USGS Earthquake API, NWS/NOAA
`api.weather.gov`, NASA FIRMS, GDACS, ReliefWeb). No proprietary or personal data is used.
This is an independent project and is **not** an official emergency-information service —
always follow guidance from local authorities.

## License

[MIT](LICENSE)
