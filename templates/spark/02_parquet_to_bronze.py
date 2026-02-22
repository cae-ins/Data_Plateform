"""
TEMPLATE SPARK : Parquet (MinIO/staging) → Iceberg Bronze
==========================================================
Usage    : Ingérer un fichier ou dossier Parquet depuis staging.
Cas type : Sorties pipelines R (panel_admin, ENE poids), données GeoAI.

Ce script est minimaliste : config injectée par le YAML Spark Operator.
À uploader sur MinIO : s3a://sparkapplication/parquet_to_bronze.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ================================================================
# <<< À MODIFIER >>>
PATH_SOURCE = "s3a://staging/mon_dossier/ma_table.parquet"
# Pour un dossier partitionné : "s3a://staging/mon_dossier/ma_table/"
TABLE_CIBLE = "nessie.bronze.ma_table"
MODE        = "overwrite"
# <<<----------->>>
# ================================================================

# --- LECTURE PARQUET ---
df = spark.read.format("parquet").load(PATH_SOURCE)

print(f"✓ Lu : {df.count():,} lignes × {len(df.columns)} colonnes")
df.printSchema()

# ================================================================
# --- TRANSFORMATIONS PRÉ-BRONZE (optionnel) ---

# ================================================================

# --- ÉCRITURE ICEBERG BRONZE ---
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

df.write \
    .format("iceberg") \
    .mode(MODE) \
    .saveAsTable(TABLE_CIBLE)

print(f"✓ Table écrite : {TABLE_CIBLE}")

spark.stop()
