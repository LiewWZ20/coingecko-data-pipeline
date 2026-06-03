# Crypto Analytics Pipeline

A production-style ELT pipeline that ingests real-time cryptocurrency data from the CoinGecko API, loads it into a Postgres data warehouse, and transforms it into analytics-ready tables using dbt. Fully orchestrated with Apache Airflow and visualized with a live Streamlit dashboard.

---

## 📐 Architecture

```
CoinGecko API
      │
      ├── /coins/markets (every 30 min)
      ├── /global (every 30 min)
      └── /coins/{id}/market_chart (daily, 90 days)
      │
      ▼
┌──────────────────────────────────────────────────────┐
│                 Apache Airflow 3.2.1                 │
│                                                      │
│  create_raw_tables                                   │
│         │                                            │
│         ▼                                            │
│  extract_coins_markets    extract_global_stats       │
│         │                        │                   │
│         ▼                        ▼                   │
│  load_coins_markets       load_global_stats          │
│         │                                            │
│         ▼                                            │
│  extract_price_history                               │
│         │                                            │
│         ▼                                            │
│  load_price_history                                  │
│         │                                            │
│         ▼                                            │
│  cleanup_old_raw_data                                │
│         │                                            │
│         ▼                                            │
│    dbt_snapshot → dbt_run → dbt_test                 │
└──────────────────────────────────────────────────────┘
      │
      ▼
┌──────────────────────────────────────────────────────┐
│              Postgres Data Warehouse                 │
│                                                      │
│  Raw Layer (append)                                  │
│  ├── raw_coins_markets   (30-min snapshots)          │
│  ├── raw_global_stats    (30-min snapshots)          │
│  └── raw_price_history   (daily OHLCV, 90 days)      │
│                                                      │
│  Staging Layer (views, deduplicated)                 │
│  ├── stg_coins_markets                               │
│  ├── stg_global_stats                                │
│  └── stg_price_history                               │
│                                                      │
│  Intermediate Layer (views, business logic)          │
│  ├── int_coin_price_analysis                         │
│  └── int_coin_rolling_metrics                        │
│                                                      │
│  Marts Layer (tables)                                │
│  ├── fct_coin_performance       (current state)      │
│  ├── fct_coin_price_history     (incremental)        │
│  └── fct_coin_rank_changes      (CDC, incremental)   │
│                                                      │
│  Snapshots (SCD Type 2)                              │
│  └── coin_rank_snapshot                              │
└──────────────────────────────────────────────────────┘
      │
      ▼
┌──────────────────────────────────────────────────────┐
│              Streamlit Dashboard                     │
│  ├── Global market overview                          │
│  ├── Top coins performance table                     │
│  ├── 90-day price chart with 7D/30D moving averages  │
│  ├── Daily returns bar chart                         │
│  ├── 7D rolling volatility chart                     │
│  └── CDC rank change history                         │
└──────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Transformation | dbt Core |
| Database/Warehouse | PostgreSQL |
| Dashboard | Streamlit + Plotly |
| Language | Python |
| Dependency Management | uv |
| Containerization | Docker + Docker Compose |
| Data Source | CoinGecko API (free tier) |

---

## 📦 Project Structure

```
coingecko_data/
├── Dockerfile                        # Custom Airflow image with uv
├── docker-compose.yaml               # All services
├── pyproject.toml                    # Dependencies managed by uv
├── uv.lock                           # Locked dependency versions
├── .env.example                      # Environment variable template
├── .gitignore
│
├── dags/
│   ├── crypto_pipeline.py            # Main Airflow DAG
│   └── utils/
│       ├── __init__.py
│       ├── lib_databse.py            # Database helpers
│       ├── lib_notifications.py      # Email alert callbacks (smtp)
│       └── lib_coingecko.py          # CoinGecko API helpers
│
├── airflow/
│   └── profiles.yml                  # dbt connection profile for Docker
│
├── streamlit/
│   ├── app.py                        # Streamlit dashboard
│   └── requirements.txt
│
└── dbt/
    └── coingecko/
        ├── dbt_project.yml
        ├── snapshots/
        │   └── coin_rank_snapshot.sql
        └── models/
            ├── staging/
            │   ├── sources.yml
            │   ├── schema.yml
            │   ├── stg_coins_markets.sql
            │   ├── stg_global_stats.sql
            │   └── stg_price_history.sql
            ├── intermediate/
            │   ├── int_coin_price_analysis.sql
            │   └── int_coin_rolling_metrics.sql
            └── marts/
                ├── schema.yml
                ├── fct_coin_performance.sql
                ├── fct_coin_price_history.sql
                └── fct_coin_rank_changes.sql
```

---

## 🔷 dbt Layer Design (8 models, 24 tests)

| Layer | Model | Materialization | Description |
|---|---|---|---|
| Staging | `stg_coins_markets` | View | Cleaned current coin market data with deduplication |
| Staging | `stg_global_stats` | View | Cleaned current global crypto market stats |
| Staging | `stg_price_history` | View | Cleaned 90-day daily price history |
| Intermediate | `int_coin_price_analysis` | View | Volatility %, ATH %, volume-to-mcap ratio |
| Intermediate | `int_coin_rolling_metrics` | View | 7D/30D rolling averages, daily returns, std dev |
| Marts | `fct_coin_performance` | Table | Current coin performance + global market context |
| Marts | `fct_coin_price_history` | Incremental | Historical prices with rolling metrics |
| Marts | `fct_coin_rank_changes` | Incremental | CDC-style rank change audit trail |

### Snapshot
`coin_rank_snapshot` — SCD Type 2 tracking coin rank, price, and market cap changes over time using dbt's `check` strategy.

---

## 🔄 CDC Implementation

Rank changes are tracked using **dbt snapshots as the change capture layer**. Every pipeline run:

1. Compares incoming coin data against the previous snapshot state
2. Closes changed records with a `dbt_valid_to` timestamp
3. Opens new records with `dbt_valid_from = now()`
4. `fct_coin_rank_changes` surfaces only meaningful rank changes with full history

**Current latency:** ~30 minutes (DAG schedule)

**Production upgrade path:** Debezium reading Postgres WAL → Kafka → sub-second latency with zero pipeline dependency

---

## ✅ Data Quality Tests

24 dbt tests covering:

- `not_null` on all key columns across all layers
- `unique` on coin identifiers
- `accepted_values` on direction column
- Referential integrity across staging and mart layers

```
PASS=24  WARN=0  ERROR=0  TOTAL=24
```

---

## 📧 Alerting

Email alerts fire on any task failure via Airflow's SMTP integration (Gmail / Microsoft 365). Alerts include DAG name, task name, run ID, and a direct link to the task logs.

![alt text](image.png)

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop (4GB+ RAM allocated)
- Python 3.11+
- uv (`powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`)
- Gmail account with App Password enabled

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/coingecko-data-pipeline.git
cd coingecko-data-pipeline
```

### 2. Set up environment
```bash
uv sync

cp .env.example .env
# Generate Fernet key
uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Add to .env as FERNET_KEY
```

### 3. Start all services
```bash
docker compose up airflow-init
docker compose up -d
```

Services started:
- Airflow UI → http://localhost:8080
- Streamlit Dashboard → http://localhost:8501
- Warehouse Postgres → localhost:5433

### 4. Add Airflow connections

In Airflow UI → Admin → Connections → Add:

**Warehouse connection:**
```
Conn Id:   crypto_warehouse
Conn Type: Postgres
Host:      crypto_warehouse
Port:      5432
Database:  crypto_db
Login:     username
Password:  password
```

**SMTP connection (for email alerts):**
```
Conn Id:    smtp_gmail
Conn Type:  SMTP
Host:       smtp.gmail.com
Port:       587
Login:      your.email@gmail.com
Password:   your-gmail-app-password
```

### 5. Trigger the pipeline
Go to http://localhost:8080 → DAGs → `crypto_pipeline` → ▶ Trigger

### 6. Run dbt locally
```bash
cd dbt/coingecko
uv run dbt run
uv run dbt test
uv run dbt docs serve --port 18080   # lineage graph at localhost:18080
```

---

## 📊 Sample Queries

**Current coin performance:**
```sql
SELECT coin_name, price_usd, market_cap_rank,
       price_change_pct_24h, volatility_pct_24h,
       coin_market_dominance_pct
FROM fct_coin_performance
ORDER BY market_cap_rank
LIMIT 5;
```

**90-day price history with rolling metrics:**
```sql
SELECT coin_name, price_date, price_usd,
       daily_return_pct, price_7d_avg, volatility_7d
FROM fct_coin_price_history
WHERE coin_id = 'bitcoin'
ORDER BY price_date DESC
LIMIT 7;
```

**Rank change history (CDC):**
```sql
SELECT coin_name, previous_rank, current_rank,
       direction, positions_moved, changed_at
FROM fct_coin_rank_changes
WHERE valid_until IS NULL
ORDER BY changed_at DESC;
```

---

## ⚠️ Known Limitations

- CoinGecko free tier updates every 60 seconds and allows 30 calls/minute — pipeline includes rate limit handling with retries
- `/coins/markets` and `/global` returns current state only — intraday history is built by polling every 30 minutes
- `/coins/{id}/market_chart` with `interval=daily` returns daily close price, not full OHLCV
- Volatility metrics are calculated from daily close prices, not high-low range
- CDC latency is ~30 minutes (batch-style, not real-time)

---

## 🔮 Production Upgrade Path

| Current (Local) | Production Equivalent |
|---|---|
| LocalExecutor | CeleryExecutor + Redis |
| Single DAG | Separate DAGs per concern with Airflow Assets |
| Postgres warehouse | Snowflake / BigQuery / AWS RDS |
| BashOperator for dbt | Astronomer Cosmos (per-model task observability) |
| dbt snapshot CDC | Debezium + Postgres WAL + Kafka |
| Docker Compose | Kubernetes / ECS / EC2 |
| Manual trigger | Scheduled + event-driven via Assets |

---

## 📚 Key Concepts Demonstrated

- **ELT pattern** — extract and load raw, transform in warehouse
- **Medallion-style layering** — raw → staging (bronze) → intermediate (silver) → marts (gold)
- **Append-only raw layer** — preserves full intraday history, deduplication in staging
- **dbt best practices** — sources, refs, tests, incremental models, snapshots, docs
- **SCD Type 2** — full historical tracking of dimension changes via dbt snapshots
- **CDC** — batch change data capture surfacing rank change audit trail
- **Incremental models** — merge strategy with watermark + active record reprocessing
- **Airflow 3.x** — TaskFlow API, dag-processor, JWT auth, SMTP alerts
- **Data quality** — 24 automated tests gate every pipeline run
- **Containerization** — full stack runs with one `docker compose up`
- **Modern Python tooling** — uv for fast, reproducible builds locally and in Docker

---

## 🙋 Author

Built by Liew Wei Zheng — [LinkedIn](https://www.linkedin.com/in/liew-wei-zheng-0224b0266/) · [GitHub](https://github.com/LiewWZ20)