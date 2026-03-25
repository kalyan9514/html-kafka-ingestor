# HTML Kafka Ingestor

A production-grade data pipeline that scrapes HTML tables from any URL, streams extracted records through Kafka, and persists them into MySQL, with dynamic schema inference, observability, and a REST API.

Built as a take-home assignment for SellWizr's Forward Deployed Engineer role.

---

## Live Deployment

This pipeline is deployed and running on Railway.

| Service | Status |
|---------|--------|
| Kafka Broker | Railway — kafka.railway.internal:29092 |
| MySQL | Railway — live, auto-created schema |
| Consumer | Live — listening for messages |
| Producer | Deployed — triggers on each run |

**CI/CD:** GitHub Actions runs build, vet, and tests on every push to main. 
Railway auto-deploys the consumer only after all CI checks pass.

**Stack:** Go 1.26 · Apache Kafka · MySQL 8.0 · Docker · Railway

---

## Architecture
```
HTTP URL
   ↓
Fetcher (retries, timeout, User-Agent)
   ↓
Parser (HTML table extraction, type inference)
   ↓
Kafka Producer → [html-records topic]
                        ↓
               Kafka Consumer
                        ↓
              MySQL (dynamic table, batch insert)
                        ↓
              Prometheus → Grafana (metrics)
                        ↓
              Dead Letter Queue (failed rows → html-records-failed)
```

---

## Features

- Fetches any URL with configurable retries and timeout
- Parses HTML wikitables and infers SQL types (INT, FLOAT, VARCHAR, DATE)
- Streams rows through Kafka with JSON serialisation
- Dynamically creates MySQL tables based on inferred schema
- Batch inserts for efficiency
- Dead Letter Queue for failed rows — no data loss
- Prometheus metrics exposed at `/metrics`
- Grafana dashboard for pipeline observability
- REST API — `POST /ingest?url=` and `GET /jobs`
- GitHub Actions CI on every push

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.26 |
| Message Queue | Apache Kafka |
| Database | MySQL 8.0 |
| Metrics | Prometheus + Grafana |
| Containerisation | Docker Compose |
| CI/CD | GitHub Actions + Railway |

---

## Prerequisites

- Go 1.26+
- Docker Desktop

---

## Running Locally

**1. Clone the repo**
```bash
git clone https://github.com/kalyan9514/html-kafka-ingestor.git
cd html-kafka-ingestor
```

**2. Create your .env file**
```bash
cp .env.example .env
```

**3. Start all services**
```bash
docker compose up -d
```

**4. Create Kafka topics**
```bash
docker exec kafka kafka-topics --create --topic html-records --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic html-records-failed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**5. Run the producer**
```bash
go run cmd/producer/main.go
```

**6. Run the consumer** (in a separate terminal)
```bash
go run cmd/consumer/main.go
```

**7. Verify data in MySQL**
```bash
docker exec -it mysql mysql -u ingestor -pingestor_pass ingestor_db -e "SELECT * FROM ingested_data LIMIT 10;"
```

**8. View metrics in Grafana**
- Open http://localhost:3000 (admin/admin)
- Go to Dashboards → Import → Upload `config/grafana/dashboard.json`

---

## Observability

| Service | URL |
|---------|-----|
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Metrics endpoint | http://localhost:2112/metrics |
| REST API | http://localhost:8080 |

**To import the dashboard:** In Grafana, go to Dashboards → Import → Upload `config/grafana/dashboard.json`

---

## REST API

**Trigger an ingestion job**
```bash
curl -X POST "http://localhost:8080/ingest?url=https://en.wikipedia.org/wiki/List_of_largest_companies_by_revenue"
```

**Check job status**
```bash
curl http://localhost:8080/jobs
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| TARGET_URL | URL to scrape | required |
| KAFKA_BROKERS | Kafka broker address | localhost:9092 |
| KAFKA_TOPIC | Kafka topic name | html-records |
| KAFKA_GROUP_ID | Consumer group ID | html-ingestor-group |
| DB_HOST | MySQL host | localhost |
| DB_PORT | MySQL port | 3306 |
| DB_USER | MySQL user | ingestor |
| DB_PASSWORD | MySQL password | required |
| DB_NAME | MySQL database | ingestor_db |
| API_PORT | REST API port | 8080 |
| METRICS_PORT | Prometheus metrics port | 2112 |

---

## Project Structure
```
├── cmd/
│   ├── producer/           # Fetches URL, parses table, publishes to Kafka
│   └── consumer/           # Reads from Kafka, inserts into MySQL
├── internal/
│   ├── fetcher/            # HTTP client with retries and timeout
│   ├── parser/             # HTML table extraction and type inference
│   ├── kafka/              # Producer, consumer, and DLQ
│   ├── db/                 # MySQL connection and batch insert
│   ├── metrics/            # Prometheus metrics
│   └── api/                # REST API endpoints
├── config/
│   ├── config.go           # Config loader
│   ├── prometheus.yml      # Prometheus scrape config
│   └── grafana/
│       └── dashboard.json  # Pre-built Grafana dashboard
├── .github/workflows/
│   └── ci.yml              # GitHub Actions CI/CD
├── Dockerfile.producer     # Docker image for producer
├── Dockerfile.consumer     # Docker image for consumer
├── docker-compose.yml      # All local services
├── .env.example            # Safe credentials template
└── .env                    # Local credentials (not committed)
```
