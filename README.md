# evm-log-injestion-service

## Overview

This service ingests ERC-20 `Transfer` events for USDC on Polygon via an HTTP webhook provided by an indexing platform. Incoming events are filtered for a configured set of monitored addresses, normalized, stored in MongoDB, and exposed via a simple HTTP query API. The service also exposes Prometheus metrics for observability and can be run locally using Docker Compose with Prometheus and Grafana.

---

## Architecture

High-level flow:

Indexer
→ HTTP Webhook
→ Listener Service  
→ Ingestion Pipeline (filter → normalize → store)  
→ MongoDB  
→ Query HTTP API  
→ Prometheus `/metrics`  
→ Grafana

Key components:

- Webhook listener for ingesting transfer events
- Buffered channel and worker pool for asynchronous processing
- MongoDB for persistence
- HTTP query API for retrieving transfers
- Prometheus metrics and Grafana dashboards

---

## Data Model

The service stores normalized ERC-20 transfer events with the following fields:

- `chain`
- `block_number`
- `transaction_hash`
- `log_index`
- `timestamp`
- `from_address`
- `to_address`
- `token_address`
- `amount_raw` (string, base units)
- `amount` (int64, base units)

Transfers are uniquely identified by `(transaction_hash, log_index)`.

---

## HTTP API

### Webhook

`POST /webhook`

Receives transfer events from the indexing platform.

Security:

- Requires a shared secret header (`X_WEBHOOK_TOKEN`)

---

### Query Transfers

`GET /query`

Query parameters:

- `address` – address to query
- `from` – start timestamp (RFC3339)
- `to` – end timestamp (RFC3339)
- `direction` – `in`, `out`, or `both`

Returns a list of matching transfers.

---

### Metrics

`GET /metrics`

Exposes Prometheus metrics in standard exposition format.

---

## Configuration

The service is configured using environment variables:

```.env
PORT
DB_URI
DB_NAME
INDEXER_WEBHOOK_TOKEN
INDEXER_API_KEY
INDEXER_URL
CONTRACT_ADDRESS
```

---

## Running Locally

Requirements:

- Docker
- Docker Compose

Start all services:

```bash docker compose up --build

```

---

## Observability

- Prometheus scrapes metrics from the listener’s `/metrics` endpoint
- Grafana is included for metrics visualization
- Metrics cover ingestion throughput, processing latency, and errors

---

## Design Decisions

- Filtering is performed in the listener service to allow dynamic address configuration and reduce coupling with the indexer
- No precomputed aggregates are stored to avoid assumptions about query patterns
- Buffered channels and a worker pool are used to handle ingestion bursts

---

## Limitations and Future Work

- Query API has no authentication
- Aggregations are computed at query time only
