# Glasgow Traders AutoPipe

A production-grade data pipeline that discovers Glasgow businesses via the Google Places API, processes them through a medallion architecture, detects changes, and auto-publishes listings to a live WordPress site.

## Architecture

```
Discover → Stage → Transform → Detect → Publish
```

| Stage | What it does |
|-------|-------------|
| **Discover** | Calls Google Places API to find Glasgow businesses by category |
| **Stage** | Lands raw JSON into PostgreSQL staging tables |
| **Transform** | dbt models clean, deduplicate, and enrich business data |
| **Detect** | SHA-256 content hashing flags new, changed, and closed businesses |
| **Publish** | Pushes verified listings to WordPress via REST API |

## Tech Stack

- **Orchestration:** Apache Airflow 2.9
- **Database:** PostgreSQL 16
- **Cache/Queue:** Redis 7
- **Transforms:** dbt (staging → intermediate → marts)
- **Containerisation:** Docker Compose
- **Infrastructure:** Terraform
- **Testing:** Great Expectations, pytest
- **APIs:** Google Places API, WordPress REST API

## Project Structure

```
glasgowtraders-autopipe/
├── dags/                  # Airflow DAG definitions
├── dbt/
│   ├── models/
│   │   ├── staging/       # Raw data cleaning
│   │   ├── intermediate/  # Business logic
│   │   └── marts/         # Final presentation layer
│   └── tests/             # dbt schema tests
├── ingestion/             # Google Places API client
├── staging/               # Database staging logic
├── publisher/             # WordPress REST API publisher
├── terraform/             # Infrastructure as code
├── tests/
│   ├── unit/              # Unit tests
│   └── great_expectations/ # Data quality checks
├── docker-compose.yml     # Local development stack
└── .gitignore
```

## Getting Started

```bash
# Clone the repo
git clone https://github.com/Larious/glasgowtraders-autopipe.git
cd glasgowtraders-autopipe

# Create your .env file with required credentials
cp .env.example .env

# Start the stack
docker compose up -d

# Verify all services are running
docker compose ps

# Access Airflow UI
open http://localhost:8080
```

## Build Progress

- [x] **Phase 00** — Prerequisites
- [x] **Phase 01** — Project Structure (scaffold, .env, Docker stack)
- [ ] **Phase 02** — Discovery Engine (Google Places API integration)
- [ ] **Phase 03** — Staging Database
- [ ] **Phase 04** — dbt Transforms
- [ ] **Phase 05** — WordPress Publisher
- [ ] **Phase 06** — Airflow DAGs
- [ ] **Phase 07** — Deploy & Monitor

## Author

**Larious Abraham Aroloye**
