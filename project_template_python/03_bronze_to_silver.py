# ============================================================
# MON_PROJET — ÉTAPE 3 : BRONZE → SILVER ICEBERG
# ============================================================
# Lit la table Bronze, applique les transformations métier :
#   - Mapping des colonnes brutes → noms standardisés
#   - Cast des types (numérique, date...)
#   - Filtres et enrichissements métier
#
# Table source   : nessie.bronze.mon_projet
# Table produite : nessie.silver.mon_projet
#
# Dépendances :
#   pip install boto3 pyspark python-dotenv pandas
# ============================================================

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

load_dotenv()

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")
NESSIE_URI       = os.getenv("NESSIE_URI",       "http://192.168.1.230:30604/api/v1")

# ============================================================
# <<< À MODIFIER >>>
TABLE_BRONZE  = "nessie.bronze.mon_projet"
TABLE_SILVER  = "nessie.silver.mon_projet"

# Mapping colonnes : {nom_bronze -> nom_silver}
# Renommer les colonnes brutes vers des noms standardisés
MAPPING_COLONNES = {
    "COL_SOURCE_1":  "colonne_standard_1",
    "COL_SOURCE_2":  "colonne_standard_2",
    # Ajouter autant que nécessaire...
}

# Colonnes à caster en numérique (Double)
COLONNES_DOUBLE = [
    "colonne_standard_1",
    # ...
]

# Colonnes à caster en entier (Integer)
COLONNES_INT = []

# Colonnes date (format attendu en entrée)
COLONNES_DATE = {
    # "nom_colonne": "format_entree"
    # "date_creation": "yyyy-MM-dd",
}

# Filtre optionnel appliqué après mapping (expression SQL)
# Laisser None pour ne pas filtrer
FILTRE_SQL = None
# Exemple : FILTRE_SQL = "statut = 'ACTIF'"
# <<<----------->>>
# ============================================================


# ============================================================
# CONNEXION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("bronze_to_silver")
    .config("spark.driver.memory", "16g")
    .config("spark.jars.packages", ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
    ]))
    .config("spark.sql.extensions", ",".join([
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    ]))
    .config("spark.sql.catalog.nessie",                "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl",   "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri",            NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref",            "main")
    .config("spark.sql.catalog.nessie.warehouse",      "s3a://bronze/")
    .config("spark.hadoop.fs.s3a.endpoint",                    MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",                  MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",                  MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",           "true")
    .config("spark.hadoop.fs.s3a.impl",                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

# ============================================================
# LECTURE BRONZE
# ============================================================

print("=" * 70)
print(f"BRONZE → SILVER : {TABLE_SILVER}")
print("=" * 70)

df = spark.table(TABLE_BRONZE)
nb_bronze = df.count()
print(f"Bronze : {nb_bronze:,} lignes × {len(df.columns)} colonnes\n")

# ============================================================
# TRANSFORMATIONS
# ============================================================

# 1. Mapping des colonnes
print("Mapping des colonnes...")
for col_src, col_dst in MAPPING_COLONNES.items():
    if col_src in df.columns:
        df = df.withColumnRenamed(col_src, col_dst)
print(f"  {len(MAPPING_COLONNES)} colonnes mappées\n")

# 2. Cast numérique
print("Cast des types numériques...")
for col in COLONNES_DOUBLE:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))
for col in COLONNES_INT:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(IntegerType()))
print(f"  {len(COLONNES_DOUBLE) + len(COLONNES_INT)} colonnes castées\n")

# 3. Cast date
for col, fmt in COLONNES_DATE.items():
    if col in df.columns:
        df = df.withColumn(col, F.to_date(F.col(col), fmt))
if COLONNES_DATE:
    print(f"  {len(COLONNES_DATE)} colonnes date converties\n")

# 4. Filtre métier
if FILTRE_SQL:
    print(f"Filtrage : {FILTRE_SQL}")
    avant = df.count()
    df = df.filter(FILTRE_SQL)
    apres = df.count()
    print(f"  {avant:,} → {apres:,} lignes ({100 * apres / avant:.1f}% conservé)\n")

# ============================================================
# <<< À MODIFIER : transformations métier supplémentaires >>>
# Exemples :
#   df = df.withColumn("annee", F.year(F.col("date_col")))
#   df = df.withColumn("montant_corrige", F.when(F.col("montant") < 0, 0).otherwise(F.col("montant")))
# <<<----------->>>
# ============================================================

# ============================================================
# ÉCRITURE EN SILVER
# ============================================================

print("=" * 70)
print(f"Écriture : {TABLE_SILVER}")
print(f"Silver   : {df.count():,} lignes × {len(df.columns)} colonnes")

df.writeTo(TABLE_SILVER).using("iceberg").tableProperty("format-version", "2").createOrReplace()

print(f"\nTable Silver écrite : {TABLE_SILVER}")
print("Prochaine étape : 04_validation_silver.py")

spark.stop()
