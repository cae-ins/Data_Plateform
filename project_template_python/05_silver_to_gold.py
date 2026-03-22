# ============================================================
# MON_PROJET — ÉTAPE 5 : SILVER → GOLD ICEBERG
# ============================================================
# Produit les tables Gold prêtes pour l'analyse
# à partir de la table Silver.
#
# Chaque requête d'agrégation produit une table Gold distincte.
# Les résultats sont aussi exportés en CSV vers staging.
#
# Dépendances :
#   pip install boto3 pyspark python-dotenv pandas
# ============================================================

import io
import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")
NESSIE_URI       = os.getenv("NESSIE_URI",       "http://192.168.1.230:30604/api/v1")

# ============================================================
# <<< À MODIFIER >>>
TABLE_SILVER  = "nessie.silver.mon_projet"
BUCKET        = "staging"
PREFIX_EXPORT = "mon_projet/exports_gold"

# Définir les tables Gold à produire :
# Chaque entrée : (nom_table_iceberg, requete_sql, nom_csv_export)
TABLES_GOLD = [
    (
        "nessie.gold.mon_projet_agreg_1",
        f"""
        SELECT
            categorie,
            COUNT(*)            AS nb_lignes,
            ROUND(SUM(valeur), 0) AS total_valeur,
            ROUND(AVG(valeur), 2) AS valeur_moyenne
        FROM nessie.silver.mon_projet
        GROUP BY categorie
        ORDER BY total_valeur DESC
        """,
        "agreg_par_categorie.csv",
    ),
    # Ajouter d'autres agrégations :
    # (
    #     "nessie.gold.mon_projet_agreg_2",
    #     f"SELECT annee, COUNT(*) AS nb FROM {TABLE_SILVER} GROUP BY annee",
    #     "agreg_par_annee.csv",
    # ),
]
# <<<----------->>>
# ============================================================


# --- CONNEXION SPARK ---

spark = (
    SparkSession.builder
    .appName("silver_to_gold")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    region_name           = "us-east-1",
    verify                = False,
)

# ============================================================
# LECTURE SILVER
# ============================================================

print("=" * 70)
print(f"SILVER → GOLD")
print("=" * 70)

nb_silver = spark.table(TABLE_SILVER).count()
print(f"Silver : {nb_silver:,} lignes\n")


def exporter_csv(df_pandas: pd.DataFrame, nom_fichier: str):
    buf = io.BytesIO()
    df_pandas.to_csv(buf, index=False, encoding="utf-8")
    cle = f"{PREFIX_EXPORT}/{nom_fichier}"
    s3.put_object(Bucket=BUCKET, Key=cle, Body=buf.getvalue(), ContentType="text/csv")
    print(f"    Export CSV : s3://{BUCKET}/{cle}")


# ============================================================
# PRODUCTION DES TABLES GOLD
# ============================================================

for nom_table, requete, nom_csv in TABLES_GOLD:
    print("=" * 70)
    print(f"Gold : {nom_table}")
    print("=" * 70)

    df_gold = spark.sql(requete)
    nb = df_gold.count()
    print(f"  {nb:,} lignes produites")

    # Écriture Iceberg
    df_gold.writeTo(nom_table).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    print(f"  Table écrite : {nom_table}")

    # Export CSV vers staging
    df_pandas = df_gold.toPandas()
    exporter_csv(df_pandas, nom_csv)
    print()

# ============================================================
# RÉSUMÉ
# ============================================================

print("=" * 70)
print("RÉSUMÉ GOLD")
print("=" * 70)

for nom_table, _, _ in TABLES_GOLD:
    nb = spark.table(nom_table).count()
    print(f"  {nom_table:<50} : {nb:,} lignes")

print(f"\nExports CSV : s3://{BUCKET}/{PREFIX_EXPORT}/")
print("\nPipeline terminé.")
print("Tables disponibles pour requêtage via Spark/Trino.")

spark.stop()
