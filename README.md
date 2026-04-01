# ETF Compliance Filing System

End-to-end ETF filing ingestion and validation platform focused on:

- high-volume data ingestion (N-PORT scale)
- correctness and idempotency
- auditable data changes
- production-ready architecture evolution (event-driven workers + review workflows)

## Why This Project

This project is designed to mirror real compliance platform requirements:

- ingest fund and holdings files (`Fund Data`, `Fund Holdings (N-Port) extract`)
- normalize and validate data
- persist filing-ready records in Postgres
- flag import exceptions for manual review
- provide a foundation for approval workflows and audit trails

---

## Architecture

### Current Architecture (Implemented)

```mermaid
flowchart TD
    A[Upload API - FastAPI] --> B[Importer Factory]
    B --> C[FundInfoImporter]
    B --> D[NPortImporter]
    C --> E[(Postgres: funds + filings)]
    D --> F[(Postgres: holdings)]
    D --> G[Chunked Processing]
    G --> H[ON CONFLICT Upsert]
    E --> I[(ExceptionImports)]
    F --> I
```

### Target Production Architecture (Roadmap)

```mermaid
flowchart TD
    UI[React Dashboard] --> API[FastAPI API]
    API --> Q[Redis Broker]
    Q --> W1[Celery Validation Workers]
    Q --> W2[Celery Ingestion Workers]
    W1 --> DB[(Postgres)]
    W2 --> DB
    DB --> AUDIT[Audit/Event Log]
    DB --> EX[Exception Queue]
    EX --> UI
    AUDIT --> UI
```

---

## Tech Stack

### Backend

- FastAPI
- SQLModel / SQLAlchemy
- Alembic
- PostgreSQL
- Pandas (data normalization + chunked ingestion)

### Frontend

- React + TypeScript + Vite (`frontend/`)
- Current UI is scaffolded and ready for dashboard implementation

---

## Data Pipeline and Ingestion

### `fund_info` pipeline

1. Parse tab-delimited file
2. Normalize required columns:
   - `ACCESSION_NUMBER`, `SERIES_ID`, `SERIES_LEI`, `SERIES_NAME`
   - `TOTAL_ASSETS`, `TOTAL_LIABILITIES`, `NET_ASSETS`
3. Upsert into:
   - `funds`
   - `filings`

### `n_port` pipeline

1. Stream file in chunks (`CHUNK_SIZE = 50,000`)
2. Normalize holdings columns
3. Resolve `filing_id` from `accession_number`
4. De-duplicate per chunk on `(filing_id, issuer_name, cusip)`
5. Upsert holdings with PostgreSQL `ON CONFLICT DO UPDATE`

This makes large imports repeatable and safer for retries.

---

## Correctness, Auditability, and Idempotency

### Correctness controls implemented

- column-level required field validation in importers
- numeric parsing/coercion for financial fields
- unique constraints:
  - filings: `(accession_number, fund_id)`
  - holdings: `(filing_id, issuer_name, cusip)`
- transaction commits by chunk for high-volume holdings loads

### Auditability foundations implemented

- `ExceptionImports` table to capture import exceptions and review status
- migration-managed schema evolution via Alembic

### Planned audit extensions

- field-level before/after audit events
- approval history (`who`, `when`, `what changed`)
- provenance tags (`source_file`, `job_id`, `row_number`)

---

## Postgres and Scaling Decisions

### Current scaling choices

- chunked ingestion for large files
- set-based upserts instead of row-by-row writes
- supporting indexes on conflict keys and foreign keys

### Production scaling roadmap

- move ingestion to Celery workers (API becomes non-blocking)
- batched jobs with retry policy and dead-letter queue
- partition large tables (time-based or filing-based) when volume grows
- add observability for ingestion latency, conflict/update rates, and error rates

---

## API Surface (Current)

- `POST /upload?fund_type=fund_info|n_port`
  - parses and imports file data
- `POST /fundtype` (placeholder)
- `POST /approve` (placeholder)
- `GET /auditlogs` (placeholder)
- `PUT /records/{recordId}` (placeholder)

---

## Data Model

Core tables:

- `funds`
- `filings`
- `holdings`
- `exceptionimports`

Migrations are in:

- `etf_filing_system/alembic/versions/`

---

## Local Development

### 1) Start Postgres

```bash
docker compose -f .docker/docker-compose.yml up -d
```

### 2) Configure environment

Create `.env` in repo root:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=etf_filing
POSTGRES_URL=postgresql+psycopg2://postgres:your_password@127.0.0.1:5432/etf_filing
```

### 3) Run migrations

```bash
cd etf_filing_system
alembic upgrade head
```

### 4) Run backend

```bash
uvicorn etf_filing_system.app:app --reload
```

### 5) Run frontend

```bash
cd frontend
npm install
npm run dev
```

---

## Repository Structure

```text
etf_filing_system/
  app.py
  models.py
  schema.py
  data_imports/
    import_factory.py
    importer.py
    fund_info_importer.py
    n_port_importer.py
  alembic/
    env.py
    versions/
frontend/
.docker/
```

---

## Current Gaps / Next Milestones

1. Move `/upload` ingestion to asynchronous workers (Celery + Redis)
2. Implement full approval workflow and reviewer actions
3. Build dashboard views:
   - import jobs
   - exception queue
   - filing history and diffs
4. Add comprehensive test suite (unit + integration + migration tests)
5. Add structured logging + metrics + alerting

---

## Project Positioning

This project intentionally combines:

- event-driven backend evolution
- high-volume financial data ingestion
- compliance-grade correctness and traceability
- practical UI and ops considerations
