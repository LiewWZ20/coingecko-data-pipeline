import time
import logging
from datetime import datetime, timedelta

import requests
import psycopg2

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
TOP_COINS = 20  # how many coins to track
DBT_PROJECT_DIR = "/opt/airflow/dbt/coingecko"
DBT_PROFILES_DIR = "/home/airflow/.dbt"


# ── Helper ───────────────────────────────────────────────────────
def get_warehouse_conn():
    """Get psycopg2 connection using Airflow connection."""
    conn = BaseHook.get_connection("crypto_warehouse")
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def notify_failure(context):
    """Send email alert on task failure."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url

    subject = f"❌ Airflow Alert: {dag_id}.{task_id} failed"
    body = f"""
    <h3>Pipeline Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Run ID:</b> {run_id}</p>
    <p><b>Logs:</b> <a href="{log_url}">Click here</a></p>
    """
    send_email(
        to="wzliew20@gmail.com",
        subject=subject,
        html_content=body,
        conn_id="smtp_gmail",
    )


# ── DAG ──────────────────────────────────────────────────────────
@dag(
    dag_id="crypto_pipeline",
    description="Extract crypto data from CoinGecko, load and transform with dbt",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
        "on_failure_callback": notify_failure,
    },
    tags=["crypto", "coingecko", "dbt"],
)
def crypto_pipeline():

    @task()
    def create_raw_tables():
        sql = """
            CREATE TABLE IF NOT EXISTS raw_coins_markets (
                id                      TEXT,
                symbol                  TEXT,
                name                    TEXT,
                current_price           NUMERIC,
                market_cap              NUMERIC,
                market_cap_rank         INTEGER,
                total_volume            NUMERIC,
                high_24h                NUMERIC,
                low_24h                 NUMERIC,
                price_change_24h        NUMERIC,
                price_change_pct_24h    NUMERIC,
                circulating_supply      NUMERIC,
                total_supply            NUMERIC,
                ath                     NUMERIC,
                ath_date                TIMESTAMP,
                last_updated            TIMESTAMP,
                extracted_at            TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS raw_global_stats (
                active_cryptocurrencies     INTEGER,
                total_market_cap_usd        NUMERIC,
                total_volume_usd            NUMERIC,
                btc_dominance               NUMERIC,
                eth_dominance               NUMERIC,
                market_cap_change_pct_24h   NUMERIC,
                extracted_at                TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS raw_price_history (
                coin_id         TEXT,
                price_date      DATE,
                price_usd       NUMERIC,
                market_cap_usd  NUMERIC,
                volume_usd      NUMERIC,
                extracted_at    TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (coin_id, price_date)   -- prevents duplicates naturally
            );
        """
        with get_warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        logger.info("Raw tables created/verified.")

    @task()
    def extract_coins_markets() -> list:
        """Fetch top N coins market data from CoinGecko."""
        url = f"{COINGECKO_BASE_URL}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_COINS,
            "page": 1,
            "sparkline": False,
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched {len(data)} coins.")
        return data

    @task()
    def extract_global_stats() -> dict:
        """Fetch global crypto market stats from CoinGecko."""
        url = f"{COINGECKO_BASE_URL}/global"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()["data"]
        logger.info("Fetched global stats.")
        return data
    
    @task()
    def extract_price_history() -> list:
        """Fetch 90 days price history for each coin."""
        # get coin ids from warehouse
        with get_warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT id FROM raw_coins_markets ORDER BY id LIMIT 5"
                )
                coin_ids = [row[0] for row in cur.fetchall()]

        logger.info(f"Fetching history for {len(coin_ids)} coins.")
        all_records = []

        for coin_id in coin_ids:
            url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"
            params = {
                "vs_currency": "usd",
                "days": 90,
                "interval": "daily",
            }
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 429:
                logger.warning(f"Rate limited on {coin_id}, sleeping 60s")
                time.sleep(60)
                response = requests.get(url, params=params, timeout=30)

            response.raise_for_status()
            data = response.json()

            prices      = {ts: val for ts, val in data["prices"]}
            volumes     = {ts: val for ts, val in data["total_volumes"]}
            market_caps = {ts: val for ts, val in data["market_caps"]}

            for ts in prices:
                all_records.append({
                    "coin_id":        coin_id,
                    "price_date":     datetime.utcfromtimestamp(ts / 1000).date().isoformat(),
                    "price_usd":      prices.get(ts),
                    "market_cap_usd": market_caps.get(ts),
                    "volume_usd":     volumes.get(ts),
                })

            # respect CoinGecko free tier rate limit (30 calls/min)
            time.sleep(5)

        logger.info(f"Fetched {len(all_records)} total price records.")
        return all_records

    @task()
    def load_coins_markets(coins: list):
        """Load coins market data into raw table."""
        truncate_sql = "TRUNCATE TABLE raw_coins_markets;"
        insert_sql = """
            INSERT INTO raw_coins_markets (
                id, symbol, name, current_price, market_cap,
                market_cap_rank, total_volume, high_24h, low_24h,
                price_change_24h, price_change_pct_24h,
                circulating_supply, total_supply, ath, ath_date,
                last_updated
            ) VALUES (
                %(id)s, %(symbol)s, %(name)s, %(current_price)s,
                %(market_cap)s, %(market_cap_rank)s, %(total_volume)s,
                %(high_24h)s, %(low_24h)s, %(price_change_24h)s,
                %(price_change_percentage_24h)s, %(circulating_supply)s,
                %(total_supply)s, %(ath)s, %(ath_date)s, %(last_updated)s
            )
        """
        with get_warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(truncate_sql)
                cur.executemany(insert_sql, coins)
            conn.commit()
        logger.info(f"Loaded {len(coins)} coins.")        

    @task()
    def load_global_stats(stats: dict):
        """Load global stats into raw table."""
        sql = """
            INSERT INTO raw_global_stats (
                active_cryptocurrencies,
                total_market_cap_usd,
                total_volume_usd,
                btc_dominance,
                eth_dominance,
                market_cap_change_pct_24h
            ) VALUES (
                %(active_cryptocurrencies)s,
                %(total_market_cap_usd)s,
                %(total_volume_usd)s,
                %(market_cap_percentage_btc)s,
                %(market_cap_percentage_eth)s,
                %(market_cap_change_percentage_24h_usd)s
            )
        """
        # flatten the nested fields we need
        flat = {
            "active_cryptocurrencies": stats["active_cryptocurrencies"],
            "total_market_cap_usd": stats["total_market_cap"]["usd"],
            "total_volume_usd": stats["total_volume"]["usd"],
            "market_cap_percentage_btc": stats["market_cap_percentage"]["btc"],
            "market_cap_percentage_eth": stats["market_cap_percentage"]["eth"],
            "market_cap_change_percentage_24h_usd": stats["market_cap_change_percentage_24h_usd"],
        }
        with get_warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE raw_global_stats;")
                cur.execute(sql, flat)
            conn.commit()
        logger.info("Loaded global stats.")

    @task()
    def load_price_history(records: list):
        """Load price history using upsert to avoid duplicates."""
        sql = """
            INSERT INTO raw_price_history (
                coin_id, price_date, price_usd, market_cap_usd, volume_usd
            ) VALUES (
                %(coin_id)s, %(price_date)s, %(price_usd)s,
                %(market_cap_usd)s, %(volume_usd)s
            )
            ON CONFLICT (coin_id, price_date)
            DO UPDATE SET
                price_usd       = EXCLUDED.price_usd,
                market_cap_usd  = EXCLUDED.market_cap_usd,
                volume_usd      = EXCLUDED.volume_usd,
                extracted_at    = NOW();
        """
        with get_warehouse_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, records)
            conn.commit()
        logger.info(f"Upserted {len(records)} price history records.")


    # ── dbt tasks ────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        on_failure_callback=notify_failure,
    )

    # ── Wire up the DAG ──────────────────────────────────────────
    raw_tables = create_raw_tables()

    coins_data = extract_coins_markets()
    global_data = extract_global_stats()

    raw_tables >> [coins_data, global_data]

    load_coins = load_coins_markets(coins_data)
    load_global = load_global_stats(global_data)

    # history runs after coins are loaded
    # (we read coin ids from the warehouse)
    history_data = extract_price_history()
    load_coins >> history_data

    load_history = load_price_history(history_data)

    [load_coins, load_global, load_history] >> dbt_run >> dbt_test

crypto_pipeline()