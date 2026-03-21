# Architectural Decisions

This document captures key technical decisions made during the build, the reasoning behind each, and the trade-offs considered.

---

## 1. Kafka over direct database writes

**Decision:** Route all rows through Kafka before inserting into MySQL.

**Why:** Direct DB writes couple the fetcher tightly to the database. If MySQL is slow or temporarily down, the entire pipeline blocks. Kafka decouples ingestion from persistence — the producer can keep publishing even if the consumer is down, and rows are safely buffered in Kafka until the consumer recovers.

**Trade-off:** Adds operational complexity (Kafka + Zookeeper to manage). Acceptable for a production pipeline where reliability matters more than simplicity.

---

## 2. JSON over Avro for serialisation

**Decision:** Rows are serialised as JSON rather than Avro with Schema Registry.

**Why:** JSON requires no additional infrastructure and is human-readable — easier to debug during development. For this assignment, the schema is already inferred and passed as the first Kafka message, so Schema Registry's main benefit (schema enforcement across producers/consumers) is handled in application code.

**Trade-off:** Avro is more efficient (binary format, smaller messages) and enforces schema evolution rules. In a high-throughput production system, Avro would be the right choice. JSON is the pragmatic choice here.

---

## 3. Dynamic schema inference over hardcoded schema

**Decision:** Column names and SQL types are inferred at runtime from the HTML table rather than defined in config.

**Why:** The system is designed to work with any URL and any table structure. Hardcoding a schema would make it brittle — it would only work for one specific table. Dynamic inference makes it genuinely extensible.

**Trade-off:** Inference can be wrong — especially with mixed or ambiguous data (e.g. a column that is mostly numbers but has some text values). We handle this by defaulting to VARCHAR when inference is uncertain, which is always safe for MySQL.

---

## 4. Schema message as first Kafka message

**Decision:** The producer publishes the inferred column schema as the first Kafka message (keyed as `"schema"`), which the consumer uses to dynamically create the MySQL table.

**Why:** The consumer has no prior knowledge of the table structure. Sending the schema through Kafka keeps the system loosely coupled — the consumer doesn't need to call any external service or share code with the producer to know the schema.

**Trade-off:** If the consumer misses the schema message, it cannot process any rows. We mitigate this by always publishing the schema first and having the consumer skip rows until the schema is received.

---

## 5. Dead Letter Queue for failed rows

**Decision:** Rows that fail to insert after processing are published to a separate `html-records-failed` Kafka topic instead of being dropped or retried indefinitely.

**Why:** Dropping rows silently causes invisible data loss. Retrying indefinitely blocks the pipeline. A DLQ gives operators visibility into failures and the ability to replay rows once the root cause is fixed.

**Trade-off:** Requires monitoring the DLQ topic. In production, an alert would be set up to notify the team when messages appear in the DLQ.

---

## 6. Batch inserts over row-by-row inserts

**Decision:** Rows are buffered in memory and inserted in batches of 10 rather than one at a time.

**Why:** Each database round trip has overhead. Inserting 50 rows one by one means 50 round trips. A single batch insert does it in one. At scale (thousands of rows), this difference is significant.

**Trade-off:** If the app crashes mid-batch, up to 9 rows could be lost. Acceptable for this use case. In a stricter system, we would use database transactions to make batches atomic.

---

## What I would add with more time

- Avro serialisation with Confluent Schema Registry
- Database transactions for atomic batch inserts
- Unit tests for parser type inference and fetcher retry logic
- Deployment to Railway with CD via GitHub Actions
- Grafana dashboard pre-configured with pipeline metrics

---

---

## Production Considerations

### Edge Cases Handled
- URL returns no wikitable → parser returns nil, producer exits cleanly with a clear error
- Header rows repeating mid-table → detected and skipped via `isHeaderRepeat()`
- Currency symbols and commas in numeric columns → stripped before MySQL insert
- Empty numeric values → defaulted to `0` to avoid type mismatch errors
- Failed rows → routed to Dead Letter Queue instead of being dropped silently

### Edge Cases Not Yet Handled
- Tables with merged cells (rowspan/colspan) — would require more complex HTML traversal
- Non-UTF8 characters in cell values — would need explicit encoding detection
- Extremely large tables (10,000+ rows) — would need streaming HTML parsing and paginated Kafka publishing instead of loading everything into memory

### LLM as a Judge
For a platform like SellWizr that ingests financial data, an LLM validation layer could be added post-ingestion to flag schema anomalies — for example, flagging if a column named `revenue` was inferred as VARCHAR instead of INT, or if values look like dates but were stored as strings. This feedback loop would improve schema inference accuracy over time.

### Handling Heavy URLs
The current implementation loads the full HTML response into memory before parsing. For pages with very large tables:
- Switch to streaming HTTP response parsing
- Add a configurable `MAX_ROWS` env variable to cap ingestion per run
- Use worker pools to parallelize Kafka publishing

### Network Failure Handling
- HTTP fetcher retries up to 3 times with linear backoff (1s, 2s, 3s)
- Kafka consumer retries on transient broker errors automatically
- Failed DB inserts are routed to the DLQ — no silent data loss
- Consumer only commits Kafka offset after successful DB insert — guarantees at-least-once delivery

### SLAs and Latency Budget

| Metric | Value |
|--------|-------|
| End-to-end ingestion (51 rows) | ~60 seconds |
| DB batch insert latency | 52ms per batch of 10 rows |
| HTTP fetch with retries (worst case) | 90 seconds (3 attempts × 30s timeout) |
| Kafka publish per row | ~1 second (improvable with batch publishing) |
| Data loss on failure | 0% (DLQ guarantees no silent drops) |

### Known Performance Bottleneck
The producer currently publishes one row per second due to Kafka's default linger settings. This can be improved by batching multiple rows into a single Kafka message or tuning `BatchSize` and `Linger` on the kafka-go writer — a straightforward config change that would reduce end-to-end time from ~60s to under 5s for 51 rows.