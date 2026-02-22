"""
TEMPLATE SPARK : Stata .dta (MinIO/staging) → Iceberg Bronze
=============================================================
Usage    : Ingérer un fichier Stata depuis staging via un pont pandas.
Cas type : Données CNPS, CAE-IPM, ENE traitées sous Stata/R/haven.

Stratégie : Spark ne lit pas natif le .dta — on télécharge via boto3
            en mémoire, on parse avec pyreadstat, on crée un DataFrame Spark.

Ce script est minimaliste : config injectée par le YAML Spark Operator.
À uploader sur MinIO : s3a://sparkapplication/dta_to_bronze.py
"""

import io
import os

import pyreadstat
from minio import Minio
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Credentials injectés par le YAML via variables d'environnement
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio.mon-namespace.svc.cluster.local:80")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# ================================================================
# <<< À MODIFIER >>>
BUCKET_SOURCE = "staging"
OBJET_SOURCE  = "cnps/donnees_traitees.dta"
TABLE_CIBLE   = "nessie.bronze.cnps_declarations"
MODE          = "overwrite"
# <<<----------->>>
# ================================================================

# --- TÉLÉCHARGEMENT .DTA ---
client = Minio(
    endpoint   = MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key = MINIO_ACCESS_KEY,
    secret_key = MINIO_SECRET_KEY,
    secure     = MINIO_ENDPOINT.startswith("https"),
)

response = client.get_object(BUCKET_SOURCE, OBJET_SOURCE)
contenu  = response.read()
response.close()
response.release_conn()

# --- PARSING STATA (driver uniquement) ---
df_pandas, meta = pyreadstat.read_dta(io.BytesIO(contenu))
print(f"✓ Stata lu : {len(df_pandas):,} lignes × {df_pandas.shape[1]} colonnes")

# --- CONVERSION EN SPARK DATAFRAME ---
df_spark = spark.createDataFrame(df_pandas)

# ================================================================
# --- TRANSFORMATIONS PRÉ-BRONZE (optionnel) ---

# ================================================================

# --- ÉCRITURE ICEBERG BRONZE ---
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

df_spark.write \
    .format("iceberg") \
    .mode(MODE) \
    .saveAsTable(TABLE_CIBLE)

print(f"✓ Table écrite : {TABLE_CIBLE}")

spark.stop()
