from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pyodbc
import pandas as pd
import logging


CONN_ID = "mmsql_docker"
CSV_PATH = "/opt/airflow/data/file 2.csv"
TARGET_TABLE = "dbo.uinque_values"  # ← ✅ UPPER_CASE


def build_pyodbc_conn_str(conn_id: str):
    c = BaseHook.get_connection(conn_id)

    extra = c.extra_dejson or {}
    driver = extra.get("driver", "ODBC Driver 18 for SQL Server")
    encrypt = extra.get("Encrypt", "no")
    trust = extra.get("TrustServerCertificate", "yes")
    
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={c.host},{c.port or 1433};"
        f"DATABASE={c.schema};"
        f"UID={c.login};"
        f"PWD={c.password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust};"
    )


def create_table_if_missed():  # ← ✅ بدون parameters
    conn_str = build_pyodbc_conn_str(CONN_ID)
    sql = f"""
    IF OBJECT_ID('{TARGET_TABLE}', 'U') IS NULL
    BEGIN
        CREATE TABLE {TARGET_TABLE} (
            id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
            customer_name NVARCHAR(150) NOT NULL,
            address NVARCHAR(250) NULL,
            birth_date DATE NULL,
            airflow_run_id NVARCHAR(250) NULL,  -- ← ✅ NVARCHAR
            loaded_at DATETIME2 DEFAULT SYSDATETIME()
        );
    END;
    """
    
    with pyodbc.connect(conn_str, timeout=30) as cn:  # ← ✅ context manager
        cn.autocommit = True
        cur = cn.cursor()
        cur.execute(sql)


def load_data(**context):
    logger = logging.getLogger("airflow.task")
    logger.info("Loading data from CSV")

    df = pd.read_csv(CSV_PATH, sep=";")
    logger.info("Missing columns check")
    
    df.columns = [str(c).strip().lower() for c in df.columns]

    expected = {"customer_name", "address", "birth_date"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

    df = df[["customer_name", "address", "birth_date"]].copy()
    df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce").dt.date
    df["airflow_run_id"] = context["run_id"]
    
    # Ensure customer_name NOT NULL / not empty
    df["customer_name"] = df["customer_name"].astype(str).str.strip()
    df = df[df["customer_name"] != ""]
    
    df = df.head(50)
    df = df.where(pd.notnull(df), None)

    records = list(df.itertuples(index=False, name=None))
    logger.info(f"Prepared {len(records)} records (first 50).")  # ← ✅ 50 مش 100

    if not records:
        logger.info("No records to insert.")
        return

    conn_str = build_pyodbc_conn_str(CONN_ID)
    insert_sql = f"""
    INSERT INTO {TARGET_TABLE} (customer_name, address, birth_date, airflow_run_id)
    VALUES (?, ?, ?, ?)
    """

    batch_size = 50
    total = len(records)

    with pyodbc.connect(conn_str, timeout=30) as cn:
        cur = cn.cursor()
        cur.fast_executemany = True

        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            cur.executemany(insert_sql, batch)
            cn.commit()
            logger.info(f"Inserted {min(i + batch_size, total)}/{total}")

        logger.info(f"✅ Done. Inserted {total} rows into {TARGET_TABLE}.")


with DAG(
    dag_id="Load_csv_file2",
    start_date=datetime(2006, 2, 1),
    schedule_interval=None,
    default_args={
        "owner": "ENG_youssef",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["csv"],  # ← ✅ list
    catchup=False,  # ← ✅ best practice
) as dag:

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_if_missed,
    )

    load_csv = PythonOperator(
        task_id="load_csv",
        python_callable=load_data,
    )

    create_table >> load_csv