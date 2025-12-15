from datetime import datetime
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


BUCKET_NAME = "YOUR_BUCKET_NAME"
RAW_PREFIX = "raw-data"
INTERMEDIATE_PREFIX = "intermediate"


# ======================================================================
#                               STEP 1
# ======================================================================
def filtrar_datos():
    s3 = boto3.client("s3")

    # -----------------------------
    # READ
    # -----------------------------
    def read_csv_from_s3(key):
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            return pd.read_csv(obj["Body"])
        except Exception as e:
            raise AirflowFailException(f"Error leyendo {key}: {e}")

    ads_views = read_csv_from_s3(f"{RAW_PREFIX}/ads_views.csv")
    advertiser_ids = read_csv_from_s3(f"{RAW_PREFIX}/advertiser_ids.csv")
    product_views = read_csv_from_s3(f"{RAW_PREFIX}/product_views.csv")

    # -----------------------------
    # VALIDATIONS
    # -----------------------------
    required_ads_cols = {"advertiser_id", "product_id", "type", "date"}
    required_prod_cols = {"advertiser_id", "product_id", "date"}
    required_adv_cols = {"advertiser_id"}

    if not required_ads_cols.issubset(ads_views.columns):
        raise AirflowFailException("ads_views.csv tiene columnas inválidas")

    if not required_prod_cols.issubset(product_views.columns):
        raise AirflowFailException("product_views.csv tiene columnas inválidas")

    if not required_adv_cols.issubset(advertiser_ids.columns):
        raise AirflowFailException("advertiser_ids.csv tiene columnas inválidas")

    if advertiser_ids.empty:
        raise AirflowFailException("La lista de advertiser_ids está vacía")

    activos = set(advertiser_ids["advertiser_id"].astype(str))

    # -----------------------------
    # FILTERING
    # -----------------------------
    ads_filtered = ads_views[ads_views["advertiser_id"].astype(str).isin(activos)]
    product_filtered = product_views[product_views["advertiser_id"].astype(str).isin(activos)]

    # -----------------------------
    # WRITE
    # -----------------------------
    def write_csv_to_s3(df, key):
        try:
            csv_buffer = df.to_csv(index=False).encode("utf-8")
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=csv_buffer,
                ContentType="text/csv"
            )
        except Exception as e:
            raise AirflowFailException(f"Error escribiendo {key}: {e}")

    write_csv_to_s3(ads_filtered, f"{INTERMEDIATE_PREFIX}/ads_views_filtered.csv")
    write_csv_to_s3(product_filtered, f"{INTERMEDIATE_PREFIX}/product_views_filtered.csv")


# ======================================================================
#                               STEP 2 — TopCTR
# ======================================================================
def calcular_top_ctr():
    s3 = boto3.client("s3")

    # -----------------------------
    # LOAD filtered ads
    # -----------------------------
    try:
        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=f"{INTERMEDIATE_PREFIX}/ads_views_filtered.csv"
        )
        df = pd.read_csv(obj["Body"])
    except Exception as e:
        raise AirflowFailException(f"No se pudo leer ads_views_filtered.csv: {e}")

    if df.empty:
        raise AirflowFailException("ads_views_filtered.csv está vacío")

    # -----------------------------
    # CALCULATE CTR
    # -----------------------------
    # Convertir event type → metric
    df["is_click"] = (df["type"] == "click").astype(int)
    df["is_impression"] = (df["type"] == "impression").astype(int)

    grouped = df.groupby(["advertiser_id", "product_id"]).agg(
        clicks=("is_click", "sum"),
        impressions=("is_impression", "sum")
    ).reset_index()

    # Evitar divisiones por cero
    grouped["ctr"] = grouped.apply(
        lambda row: row["clicks"] / row["impressions"] if row["impressions"] > 0 else 0,
        axis=1
    )

    # -----------------------------
    # TOP 20 POR ADVERTISER
    # -----------------------------
    grouped["rank"] = grouped.groupby("advertiser_id")["ctr"].rank(
        method="first", ascending=False
    )

    top_ctr = grouped[grouped["rank"] <= 20].copy()
    top_ctr = top_ctr.sort_values(["advertiser_id", "ctr"], ascending=[True, False])

    # -----------------------------
    # WRITE TO S3
    # -----------------------------
    try:
        csv_buffer = top_ctr.to_csv(index=False).encode("utf-8")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{INTERMEDIATE_PREFIX}/top_ctr.csv",
            Body=csv_buffer,
            ContentType="text/csv"
        )
    except Exception as e:
        raise AirflowFailException(f"Error escribiendo top_ctr.csv: {e}")

# ======================================================================
#                               STEP 3 — TopProduct
# ======================================================================
def calcular_top_product():
    s3 = boto3.client("s3")

    # -----------------------------
    # LOAD filtered product views
    # -----------------------------
    try:
        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=f"{INTERMEDIATE_PREFIX}/product_views_filtered.csv"
        )
        df = pd.read_csv(obj["Body"])
    except Exception as e:
        raise AirflowFailException(f"No se pudo leer product_views_filtered.csv: {e}")

    if df.empty:
        raise AirflowFailException("product_views_filtered.csv está vacío")

    # -----------------------------
    # CALCULATE MOST VIEWED PRODUCTS
    # -----------------------------
    grouped = df.groupby(["advertiser_id", "product_id"]).agg(
        views=("product_id", "count")
    ).reset_index()

    # -----------------------------
    # TOP 20 POR ADVERTISER
    # -----------------------------
    grouped["rank"] = grouped.groupby("advertiser_id")["views"].rank(
        method="first", ascending=False
    )

    top_product = grouped[grouped["rank"] <= 20].copy()
    top_product = top_product.sort_values(["advertiser_id", "views"], ascending=[True, False])

    # -----------------------------
    # WRITE TO S3
    # -----------------------------
    try:
        csv_buffer = top_product.to_csv(index=False).encode("utf-8")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{INTERMEDIATE_PREFIX}/top_product.csv",
            Body=csv_buffer,
            ContentType="text/csv"
        )
    except Exception as e:
        raise AirflowFailException(f"Error escribiendo top_product.csv: {e}")
    

# ======================================================================
#                           STEP 4 — DBWriting
# ======================================================================
import psycopg2
from psycopg2.extras import execute_values


def escribir_en_db(ds, **context):
    """
    Lee top_ctr.csv y top_product.csv desde S3 y los escribe en RDS PostgreSQL.
    - Limpia tabla 'recommendations' del día
    - Inserta valores nuevos
    - Inserta también en 'recommendations_history' con fecha = ds
    """

    s3 = boto3.client("s3")

    def read_csv(key):
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            return pd.read_csv(obj["Body"])
        except Exception as e:
            raise AirflowFailException(f"Error al leer {key}: {e}")

    # Load both model outputs
    top_ctr = read_csv(f"{INTERMEDIATE_PREFIX}/top_ctr.csv")
    top_product = read_csv(f"{INTERMEDIATE_PREFIX}/top_product.csv")

    if top_ctr.empty and top_product.empty:
        raise AirflowFailException("No hay datos para escribir en DB.")

    # Add model column
    top_ctr["model"] = "TOPCTR"
    top_product["model"] = "TOPPRODUCT"

    # Unify schema
    df_all = pd.concat([top_ctr, top_product], ignore_index=True)

    expected_cols = {"advertiser_id", "product_id", "rank", "model"}
    if not expected_cols.issubset(df_all.columns):
        raise AirflowFailException("Columnas faltantes en los datos antes de escribir en DB.")

    # Connect to Postgres RDS
    try:
        conn = psycopg2.connect(
            host="YOUR_RDS_HOST",
            user="YOUR_DB_USER",
            password="YOUR_DB_PASS",
            dbname="YOUR_DB_NAME"
        )
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as e:
        raise AirflowFailException(f"Error conectando a la base RDS: {e}")

    try:
        # -----------------------------------------------------
        # 1. Limpia tabla recommendations del día
        # -----------------------------------------------------
        cur.execute("DELETE FROM recommendations;")

        # -----------------------------------------------------
        # 2. Inserta recomendaciones (día actual)
        # -----------------------------------------------------
        records_reco = [
            (
                row["advertiser_id"],
                row["model"],
                row["product_id"],
                int(row["rank"])
            )
            for _, row in df_all.iterrows()
        ]

        execute_values(
            cur,
            """
            INSERT INTO recommendations (advertiser_id, model, product_id, rank)
            VALUES %s;
            """,
            records_reco
        )

        # -----------------------------------------------------
        # 3. Inserta en tabla histórica
        # -----------------------------------------------------
        records_hist = [
            (
                row["advertiser_id"],
                row["model"],
                row["product_id"],
                int(row["rank"]),
                ds  # historiza con la fecha de ejecución del DAG
            )
            for _, row in df_all.iterrows()
        ]

        execute_values(
            cur,
            """
            INSERT INTO recommendations_history
                (advertiser_id, model, product_id, rank, date)
            VALUES %s;
            """,
            records_hist
        )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise AirflowFailException(f"Error escribiendo en la base de datos: {e}")
    

# ======================================================================
#               STEP 5 — mover archivos a processed
# ======================================================================
def mover_a_processed(ds, **context):

    s3 = boto3.client("s3")

    # Archivos que se deben mover de raw-data/
    raw_files = [
        "ads_views.csv",
        "advertiser_ids.csv",
        "product_views.csv"
    ]

    # Archivos que se deben mover de intermediate/
    intermediate_files = [
        "ads_views_filtered.csv",
        "product_views_filtered.csv",
        "top_ctr.csv",
        "top_product.csv"
    ]

    # -----------------------------
    # Función utilidad
    # -----------------------------
    def move_file(src_key, dst_key):
        try:
            # Copy
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={"Bucket": BUCKET_NAME, "Key": src_key},
                Key=dst_key
            )
            # Delete original
            s3.delete_object(Bucket=BUCKET_NAME, Key=src_key)

        except Exception as e:
            raise AirflowFailException(f"Error moviendo {src_key} → {dst_key}: {e}")

    # -----------------------------
    # Mover raw-data → processed/raw-data/ds/
    # -----------------------------
    for f in raw_files:
        src = f"{RAW_PREFIX}/{f}"
        dst = f"processed/raw-data/{ds}/{f}"
        move_file(src, dst)

    # -----------------------------
    # Mover intermediate → processed/intermediate/ds/
    # -----------------------------
    for f in intermediate_files:
        src = f"{INTERMEDIATE_PREFIX}/{f}"
        dst = f"processed/intermediate/{ds}/{f}"
        move_file(src, dst)

# ======================================================================
#                           DAG DEFINITION
# ======================================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_diario",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["mlops", "adtech"],
) as dag:

    filtrar_datos_task = PythonOperator(
        task_id="filtrar_datos",
        python_callable=filtrar_datos,
    )

    calcular_top_ctr_task = PythonOperator(
        task_id="calcular_top_ctr",
        python_callable=calcular_top_ctr,
    )

    calcular_top_product_task = PythonOperator(
        task_id="calcular_top_product",
        python_callable=calcular_top_product,
    )

    escribir_en_db_task = PythonOperator(
        task_id="escribir_en_db",
        python_callable=escribir_en_db,
        op_kwargs={"ds": "{{ ds }}"},
    )

    mover_a_processed_task = PythonOperator(
        task_id="mover_a_processed",
        python_callable=mover_a_processed,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # DEPENDENCIAS FINALES
    filtrar_datos_task \
        >> calcular_top_ctr_task \
        >> calcular_top_product_task \
        >> escribir_en_db_task \
        >> mover_a_processed_task

