"""
TEMPLATE SPARK : Bronze → Silver (transformation SQL)
=====================================================
Usage    : Transformer une table Bronze en table Silver.
           Silver = données propres, typées, avec règles métier appliquées.
           La logique métier est exprimée en SQL Spark.

Ce script est minimaliste : config injectée par le YAML Spark Operator.
À uploader sur MinIO : s3a://sparkapplication/bronze_to_silver.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# ================================================================
# <<< À MODIFIER >>>
TABLE_BRONZE = "nessie.bronze.ma_table"
TABLE_SILVER = "nessie.silver.ma_table"
MODE         = "overwrite"  # "overwrite" (reconstruction complète) ou "append"
# <<<----------->>>
# ================================================================

# --- LECTURE BRONZE ---
df_bronze = spark.table(TABLE_BRONZE)
print(f"✓ Bronze lu : {df_bronze.count():,} lignes")

# ================================================================
# --- TRANSFORMATIONS SILVER ---
# Exemple générique — adapter à vos données

df_silver = (
    df_bronze

    # 1. Cast des types (Bronze = tout String)
    # .withColumn("montant_brut", F.col("montant_brut").cast("double"))
    # .withColumn("periode",      F.col("periode").cast("date"))

    # 2. Nettoyage
    # .withColumn("matricule", F.trim(F.upper(F.col("matricule"))))

    # 3. Filtrage des lignes invalides
    # .filter(F.col("matricule").isNotNull())
    # .filter(F.col("montant_brut") > 0)

    # 4. Colonnes calculées
    # .withColumn("annee",  F.year("periode"))
    # .withColumn("mois",   F.month("periode"))

    # 5. Déduplication
    # .dropDuplicates(["matricule", "periode"])
)

# --- ALTERNATIVE : Transformation via SQL pur ---
# df_bronze.createOrReplaceTempView("bronze_source")
# df_silver = spark.sql("""
#     SELECT
#         TRIM(UPPER(matricule))  AS matricule,
#         CAST(montant_brut AS DOUBLE) AS montant_brut,
#         CAST(montant_net  AS DOUBLE) AS montant_net,
#         CAST(periode AS DATE)        AS periode,
#         YEAR(CAST(periode AS DATE))  AS annee
#     FROM bronze_source
#     WHERE matricule IS NOT NULL
#       AND CAST(montant_brut AS DOUBLE) > 0
# """)

# ================================================================

print(f"✓ Silver prêt : {df_silver.count():,} lignes")

# --- ÉCRITURE ICEBERG SILVER ---
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

df_silver.write \
    .format("iceberg") \
    .mode(MODE) \
    .saveAsTable(TABLE_SILVER)

print(f"✓ Table écrite : {TABLE_SILVER}")

spark.stop()
