import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Page config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Analytics",
    page_icon="🪙",
    layout="wide",
)

# ── DB Connection ─────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="crypto_warehouse",
        port=5432,
        dbname="crypto_db",
        user="warehouse_user",
        password="warehouse_pass",
    )

@st.cache_data(ttl=300)  # cache for 5 minutes
def run_query(sql):
    conn = get_connection()
    return pd.read_sql(sql, conn)

# ── Header ────────────────────────────────────────────────────────
st.title("🪙 Crypto Analytics Dashboard")
st.caption(f"Data refreshes every 5 minutes · Last loaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── Global Market Stats ───────────────────────────────────────────
st.subheader("🌍 Global Market Overview")

global_df = run_query("""
    SELECT
        total_market_cap_usd,
        btc_dominance_pct,
        eth_dominance_pct,
        market_cap_change_pct_24h,
        extracted_at
    FROM stg_global_stats
    ORDER BY extracted_at DESC
    LIMIT 1
""")

if not global_df.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Total Market Cap",
        f"${global_df['total_market_cap_usd'].iloc[0]/1e12:.2f}T",
    )
    col2.metric(
        "BTC Dominance",
        f"{global_df['btc_dominance_pct'].iloc[0]:.1f}%",
    )
    col3.metric(
        "ETH Dominance",
        f"{global_df['eth_dominance_pct'].iloc[0]:.1f}%",
    )
    col4.metric(
        "24h Market Change",
        f"{global_df['market_cap_change_pct_24h'].iloc[0]:.2f}%",
        delta=f"{global_df['market_cap_change_pct_24h'].iloc[0]:.2f}%",
    )

st.divider()

# ── Top Coins Table ───────────────────────────────────────────────
st.subheader("📊 Top Coins Performance")

coins_df = run_query("""
    SELECT
        market_cap_rank        as rank,
        coin_name              as name,
        coin_symbol            as symbol,
        price_usd,
        price_change_pct_24h   as change_24h,
        market_cap_usd,
        volume_24h_usd         as volume_24h,
        volatility_pct_24h     as volatility,
        pct_of_ath,
        coin_market_dominance_pct as dominance
    FROM fct_coin_performance
    ORDER BY rank
""")

if not coins_df.empty:
    # format columns
    coins_display = coins_df.copy()
    coins_display["price_usd"]      = coins_display["price_usd"].apply(lambda x: f"${x:,.2f}")
    coins_display["change_24h"]     = coins_display["change_24h"].apply(lambda x: f"{x:+.2f}%")
    coins_display["market_cap_usd"] = coins_display["market_cap_usd"].apply(lambda x: f"${x/1e9:.1f}B")
    coins_display["volume_24h"]     = coins_display["volume_24h"].apply(lambda x: f"${x/1e9:.1f}B")
    coins_display["volatility"]     = coins_display["volatility"].apply(lambda x: f"{x:.2f}%")
    coins_display["pct_of_ath"]     = coins_display["pct_of_ath"].apply(lambda x: f"{x:.1f}%")
    coins_display["dominance"]      = coins_display["dominance"].apply(lambda x: f"{x:.2f}%")

    st.dataframe(
        coins_display,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ── Price History Chart ───────────────────────────────────────────
st.subheader("📈 Price History")

available_coins = run_query("""
    SELECT DISTINCT coin_name, coin_id
    FROM fct_coin_price_history
    ORDER BY coin_name
""")

selected_coin = st.selectbox(
    "Select coin",
    options=available_coins["coin_name"].tolist(),
    index=0,
)

coin_id = available_coins[
    available_coins["coin_name"] == selected_coin
]["coin_id"].iloc[0]

history_df = run_query(f"""
    SELECT
        price_date,
        price_usd,
        price_7d_avg,
        price_30d_avg,
        volume_usd,
        daily_return_pct,
        volatility_7d
    FROM fct_coin_price_history
    WHERE coin_id = '{coin_id}'
    ORDER BY price_date
""")

if not history_df.empty:
    # price + moving averages
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=history_df["price_date"],
        y=history_df["price_usd"],
        name="Price",
        line=dict(color="#f7931a", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=history_df["price_date"],
        y=history_df["price_7d_avg"],
        name="7D MA",
        line=dict(color="#00d4ff", width=1.5, dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=history_df["price_date"],
        y=history_df["price_30d_avg"],
        name="30D MA",
        line=dict(color="#ff6b6b", width=1.5, dash="dot"),
    ))

    fig.update_layout(
        title=f"{selected_coin} Price (90 Days)",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        hovermode="x unified",
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)

    # daily returns + volatility
    col1, col2 = st.columns(2)

    with col1:
        fig2 = px.bar(
            history_df,
            x="price_date",
            y="daily_return_pct",
            title="Daily Returns (%)",
            color="daily_return_pct",
            color_continuous_scale=["#ff6b6b", "#ffffff", "#00ff88"],
            color_continuous_midpoint=0,
            template="plotly_dark",
        )
        fig2.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        fig3 = px.line(
            history_df,
            x="price_date",
            y="volatility_7d",
            title="7D Rolling Volatility",
            template="plotly_dark",
            color_discrete_sequence=["#f7931a"],
        )
        fig3.update_layout(height=300)
        st.plotly_chart(fig3, use_container_width=True)

# ── Coin Rankings Snapshot ────────────────────────────────────────
st.divider()
st.subheader("🏆 Current Market Rankings")

snapshot_df = run_query("""
    SELECT
        market_cap_rank as rank,
        coin_name       as name,
        coin_symbol     as symbol,
        price_usd,
        market_cap_usd,
        dbt_valid_from  as tracked_since
    FROM coin_rank_snapshot
    WHERE dbt_valid_to is null
    ORDER BY market_cap_rank
    LIMIT 20
""")

if not snapshot_df.empty:
    snapshot_df["price_usd"]      = snapshot_df["price_usd"].apply(lambda x: f"${x:,.2f}")
    snapshot_df["market_cap_usd"] = snapshot_df["market_cap_usd"].apply(lambda x: f"${x/1e9:.1f}B")
    snapshot_df["tracked_since"]  = pd.to_datetime(snapshot_df["tracked_since"]).dt.strftime("%Y-%m-%d")

    st.dataframe(snapshot_df, use_container_width=True, hide_index=True)

# ── CDC Rank Changes ──────────────────────────────────────────────
st.divider()
st.subheader("🔄 Rank Change History (CDC)")
st.caption("Powered by dbt snapshots — captures every rank change as it happens")

changes_df = run_query("""
    SELECT
        changed_at,
        coin_name       as coin,
        coin_symbol     as symbol,
        previous_rank,
        current_rank,
        direction,
        positions_moved,
        price_at_change
    FROM fct_coin_rank_changes
    ORDER BY changed_at DESC
    LIMIT 50
""")

if changes_df.empty:
    st.info("No rank changes captured yet — changes appear here after the next pipeline run detects a difference.")
else:
    changes_df["price_at_change"] = changes_df["price_at_change"].apply(
        lambda x: f"${x:,.2f}"
    )
    changes_df["changed_at"] = pd.to_datetime(
        changes_df["changed_at"]
    ).dt.strftime("%Y-%m-%d %H:%M")

    st.dataframe(changes_df, use_container_width=True, hide_index=True)

    # summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Changes Captured", len(changes_df))
    col2.metric(
        "Coins Moving Up",
        len(changes_df[changes_df["direction"] == "📈 UP"])
    )
    col3.metric(
        "Coins Moving Down",
        len(changes_df[changes_df["direction"] == "📉 DOWN"])
    )

st.caption("Built with Airflow · dbt · PostgreSQL · Streamlit")