# Crypto Analytics Pipeline

A production-style ELT pipeline that ingests real-time cryptocurrency data from the CoinGecko API, loads it into a Postgres data warehouse, and transforms it into analytics-ready tables using dbt — fully orchestrated with Apache Airflow.

---

## 📐 Architecture

```
CoinGecko API
      │
      ▼
┌─────────────────────────────────────────────┐
│              Apache Airflow 3.2.1           │
│                                             │
│  create_raw_tables                          │
│         │                                   │
│         ▼                                   │
│  extract_coins_markets  extract_global_stats│
│         │                       │           │
│         ▼                       ▼           │
│  load_coins_markets   load_global_stats     │
│         │                       │           │
│         └───────────┬───────────┘           │
│                     ▼                       │
│                  dbt_run                    │
│                     │                       │
│                     ▼                       │
│                  dbt_test                   │
└─────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────┐
│           Postgres Data Warehouse           │
│                                             │
│  raw_coins_markets    raw_global_stats      │
│         │                    │              │
│         ▼                    ▼              │
│  stg_coins_markets   stg_global_stats       │
│         │                    │              │
│         ▼                    │              │
│  int_coin_price_analysis     │              │
│         │                    │              │
│         └──────────┬─────────┘              │
│                    ▼                        │
│           fct_coin_performance              │
└─────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.2.1 |
| Transformation | dbt Core 1.11.9 |
| Warehouse | PostgreSQL 16 |
| Language | Python 3.11 |
| Dependency Management | uv |
| Containerization | Docker + Docker Compose |
| Data Source | CoinGecko API (free tier) |

---

## 📦 Project Structure

```
coingecko_data/
├── Dockerfile                      # Custom Airflow image with uv
├── docker-compose.yaml             # Airflow + Postgres services
├── pyproject.toml                  # Dependencies managed by uv
├── .env                            # Environment variables (not committed)
│
├── dags/
│   └── crypto_pipeline.py          # Main Airflow DAG
│
├── airflow/
│   └── profiles.yml                # dbt connection profile for Docker
│
└── dbt/
    └── coingecko/
        ├── dbt_project.yml
        └── models/
            ├── staging/
            │   ├── sources.yml
            │   ├── schema.yml
            │   ├── stg_coins_markets.sql
            │   └── stg_global_stats.sql
            ├── intermediate/
            │   └── int_coin_price_analysis.sql
            └── marts/
                ├── schema.yml
                └── fct_coin_performance.sql
```

---

## 🔷 dbt Layer Design

### Staging
Clean 1-to-1 representations of raw source tables. Casts types, renames columns, no business logic.

- `stg_coins_markets` — cleaned coin market data (price, volume, market cap)
- `stg_global_stats` — cleaned global crypto market stats

### Intermediate
Business logic and derived metrics built on staging models.

- `int_coin_price_analysis` — adds volatility %, price range, % of ATH, volume-to-market-cap ratio

### Marts
Final analytics-ready tables joining coins with global market context.

- `fct_coin_performance` — fact table with full coin performance + market dominance %

---

## ✅ Data Quality Tests

13 dbt tests covering:

- `not_null` on all key columns
- `unique` on coin identifiers
- Referential integrity across layers

```
PASS=13  WARN=0  ERROR=0  TOTAL=13
```

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
- uv (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/coingecko_data.git
cd coingecko_data
```

### 2. Set up environment
```bash
# Install local dependencies
uv sync

# Create .env file
cp .env.example .env
# Edit .env and add your FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Start services
```bash
docker compose up airflow-init
docker compose up -d
```

### 4. Add Airflow connection
In Airflow UI (http://localhost:8080) → Admin → Connections → Add:
```
Conn Id:   crypto_warehouse
Conn Type: Postgres
Host:      crypto_warehouse
Port:      5432
Database:  crypto_db
Login:     warehouse_user
Password:  warehouse_pass
```

### 5. Trigger the pipeline
Go to http://localhost:8080 → DAGs → `crypto_pipeline` → ▶ Trigger

### 6. Run dbt locally
```bash
cd dbt/coingecko
uv run dbt run
uv run dbt test
uv run dbt docs serve --port 18080
```

---

## 📊 Sample Output

```sql
SELECT coin_name, price_usd, market_cap_rank,
       price_change_pct_24h, volatility_pct_24h,
       coin_market_dominance_pct
FROM fct_coin_performance
ORDER BY market_cap_rank
LIMIT 5;
```

```
coin_name  | price_usd  | rank | change_24h | volatility | dominance
-----------+------------+------+------------+------------+----------
Bitcoin    | 103241.00  |  1   |   +2.34%   |   3.21%    |  54.32%
Ethereum   |   2451.00  |  2   |   +1.87%   |   4.15%    |  11.24%
Tether     |      1.00  |  3   |   +0.01%   |   0.02%    |   5.43%
BNB        |    645.00  |  4   |   +1.23%   |   2.87%    |   3.21%
Solana     |    172.00  |  5   |   +3.45%   |   5.67%    |   2.14%
```

---

## 🔮 Production Upgrade Path

This project uses patterns that map directly to production:

| Current (Local) | Production Equivalent |
|---|---|
| LocalExecutor | CeleryExecutor + Redis |
| Postgres warehouse | Snowflake / BigQuery |
| BashOperator for dbt | Astronomer Cosmos |
| Manual trigger | Scheduled + event-driven |
| Docker Compose | Kubernetes / ECS |

---

## 📚 Key Concepts Demonstrated

- **ELT pattern** — extract and load raw, transform in warehouse
- **Medallion-style layering** — raw → staging → intermediate → marts
- **dbt best practices** — sources, refs, tests, documentation
- **Airflow 3.x DAG authoring** — TaskFlow API, dependency wiring
- **Data quality** — automated tests gate every pipeline run
- **Containerization** — full stack runs with one `docker compose up`
- **Modern Python tooling** — uv for fast, reproducible builds

---

## dbt Models (8 models, 24 tests)

| Layer | Model | Description |
|---|---|---|
| Staging | stg_coins_markets | Cleaned coin market data |
| Staging | stg_global_stats | Cleaned global market stats |
| Staging | stg_price_history | Cleaned 90-day price history |
| Intermediate | int_coin_price_analysis | Volatility, ATH %, volume ratios |
| Intermediate | int_coin_rolling_metrics | 7D/30D rolling averages, daily returns |
| Marts | fct_coin_performance | Current coin performance + market context |
| Marts | fct_coin_price_history | Historical prices with rolling metrics (incremental) |
| Marts | fct_coin_rank_changes | CDC-style rank change audit trail (incremental) |

### CDC Implementation
Rank changes are tracked using dbt snapshots as the change capture layer.
Every pipeline run compares incoming data against the previous snapshot state
using a checksum strategy — closing changed records with a `dbt_valid_to`
timestamp and opening new ones. This provides a full audit trail of every
rank change with ~1 hour latency on the current schedule.

Production upgrade path: Debezium reading Postgres WAL → Kafka →
sub-second latency with zero pipeline dependency.

## 🙋 Author

Built by Liew Wei Zheng — [LinkedIn](https://www.linkedin.com/in/liew-wei-zheng-0224b0266/) · [GitHub](https://github.com/LiewWZ20)