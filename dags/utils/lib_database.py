import psycopg2
from airflow.sdk.bases.hook import BaseHook


def get_warehouse_conn():
    """Get psycopg2 connection using Airflow connection store."""
    conn = BaseHook.get_connection("crypto_warehouse")
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

    
def execute_sql(sql: str, params=None):
    """Execute a single SQL statement."""
    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def executemany_sql(sql: str, records: list):
    """Execute SQL for multiple records."""
    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)
        conn.commit()