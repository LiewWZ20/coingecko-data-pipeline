import os
import time
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")

def fetch_coins_markets(vs_currency: str = "usd", per_page: int = 20) -> list:
    """Fetch top N coins market data from CoinGecko."""
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": False,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    logger.info(f"Fetched {len(data)} coins from CoinGecko.")
    return data


def fetch_global_stats() -> dict:
    """Fetch global crypto market stats from CoinGecko."""
    url = f"{BASE_URL}/global"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()["data"]
    logger.info("Fetched global stats.")
    return data


def fetch_price_history(
    coin_id: str,
    days: int = 90,
    vs_currency: str = "usd"
) -> list:
    """Fetch daily price history for a single coin."""
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days,
        "interval": "daily",
    }
    response = requests.get(url, params=params, timeout=30)

    # Coingecko Public API have a rate limit of 50 calls/minute.
    # If we hit the limit, wait and retry once.
    if response.status_code == 429:
        logger.warning(f"Rate limited on {coin_id}, sleeping 60s")
        time.sleep(60)
        response = requests.get(url, params=params, timeout=30)

    response.raise_for_status()
    data = response.json()

    prices = {ts: val for ts, val in data["prices"]}
    volumes = {ts: val for ts, val in data["total_volumes"]}
    market_caps = {ts: val for ts, val in data["market_caps"]}

    records = []
    for ts in prices:
        records.append({
            "coin_id": coin_id,
            "price_date": datetime.utcfromtimestamp(ts / 1000).date().isoformat(),
            "price_usd": prices.get(ts),
            "market_cap_usd": market_caps.get(ts),
            "volume_usd": volumes.get(ts),
        })

    logger.info(f"Fetched {len(records)} records for {coin_id}.")
    return records