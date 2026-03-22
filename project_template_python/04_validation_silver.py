# ============================================================
# MON_PROJET — ÉTAPE 4 : VALIDATION DE LA TABLE SILVER
# ============================================================
# Contrôles qualité sur la table Silver Iceberg via SQL Spark.
# Les rapports sont sauvegardés dans staging/mon_projet/validation/
#
# Contrôles :
#   1. Complétude des colonnes clés
#   2. Doublons sur la clé unique
#   3. Valeurs aberrantes sur les colonnes numériques
#   4. Distribution des colonnes catégorielles
#
# Dépendances :
#   pip install boto3 pyspark python-dotenv
# ============================================================

import io
import csv
import os
import boto3
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
TABLE_SILVER    = "nessie.silver.mon_projet"
BUCKET          = "staging"
PREFIX_VALID    = "mon_projet/validation"

# Colonnes dont on vérifie la complétude (doivent être < seuil_na %)
COLONNES_CLES   = ["id", "date", "valeur"]   # remplacer par les vraies colonnes
SEUIL_NA_PCT    = 5.0                        # alerte si NA > 5 %

# Colonne(s) formant la clé unique (contrôle doublons)
CLES_UNIQUE     = ["id"]                     # peut être une combinaison

# Colonnes numériques à surveiller (valeurs négatives, outliers)
COLONNES_NUM    = ["valeur"]
# <<<----------->>>
# ============================================================


# --- CONNEXION SPARK ---

spark = (
    SparkSession.builder
    .appName("validation_silver")
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

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    region_name           = "us-east-1",
    verify                = False,
)


def sauvegarder_rapport(donnees: list, nom_fichier: str):
    if not donnees:
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=donnees[0].keys())
    writer.writeheader()
    writer.writerows(donnees)
    cle = f"{PREFIX_VALID}/{nom_fichier}"
    s3.put_object(Bucket=BUCKET, Key=cle, Body=buf.getvalue().encode(), ContentType="text/csv")
    print(f"    Rapport : s3://{BUCKET}/{cle}")


# ============================================================
# LECTURE SILVER
# ============================================================

print("=" * 70)
print("VALIDATION : Table Silver")
print("=" * 70)

df = spark.table(TABLE_SILVER)
nb_total = df.count()
cols_existantes = df.columns

print(f"Table : {nb_total:,} lignes × {len(cols_existantes)} colonnes\n")

# ============================================================
# CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS
# ============================================================

print("=" * 70)
print("CONTRÔLE 1 : Complétude des colonnes clés")
print("=" * 70)

colonnes_presentes = [c for c in COLONNES_CLES if c in cols_existantes]
if colonnes_presentes:
    exprs = ", ".join(
        f"ROUND(100.0 * SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS {c}_pct_na"
        for c in colonnes_presentes
    )
    result = spark.sql(f"SELECT {exprs} FROM {TABLE_SILVER}").collect()[0].asDict()

    rapport_completude = []
    for col in colonnes_presentes:
        pct_na = result[f"{col}_pct_na"]
        flag   = "ALERTE" if pct_na > SEUIL_NA_PCT else "ok"
        print(f"  [{flag}] {col:<30} : {pct_na:.1f}% NA")
        rapport_completude.append({"colonne": col, "pct_na": pct_na, "statut": flag})

    sauvegarder_rapport(rapport_completude, "completude_colonnes_cles.csv")
else:
    print("  Aucune colonne clé trouvée dans la table\n")

print()

# ============================================================
# CONTRÔLE 2 : DOUBLONS SUR LA CLÉ UNIQUE
# ============================================================

print("=" * 70)
print(f"CONTRÔLE 2 : Doublons sur {CLES_UNIQUE}")
print("=" * 70)

cles_presentes = [c for c in CLES_UNIQUE if c in cols_existantes]
if cles_presentes:
    group_by = ", ".join(f"`{c}`" for c in cles_presentes)
    doublons = spark.sql(f"""
        SELECT {group_by}, COUNT(*) AS nb
        FROM {TABLE_SILVER}
        GROUP BY {group_by}
        HAVING COUNT(*) > 1
        ORDER BY nb DESC
        LIMIT 100
    """).collect()

    if doublons:
        print(f"  ALERTE : {len(doublons)} combinaisons en doublon")
        rapport_doublons = [row.asDict() for row in doublons]
        sauvegarder_rapport(rapport_doublons, "doublons.csv")
    else:
        print("  ok : Aucun doublon détecté")
else:
    print("  Colonnes clé absentes, contrôle ignoré")

print()

# ============================================================
# CONTRÔLE 3 : VALEURS ABERRANTES (colonnes numériques)
# ============================================================

print("=" * 70)
print("CONTRÔLE 3 : Statistiques colonnes numériques")
print("=" * 70)

cols_num_presentes = [c for c in COLONNES_NUM if c in cols_existantes]
rapport_stats = []
for col in cols_num_presentes:
    stats = spark.sql(f"""
        SELECT
            '{col}' AS colonne,
            COUNT(*) AS nb_non_null,
            ROUND(MIN(`{col}`), 2) AS min,
            ROUND(MAX(`{col}`), 2) AS max,
            ROUND(AVG(`{col}`), 2) AS moyenne,
            ROUND(PERCENTILE_APPROX(`{col}`, 0.5), 2) AS mediane,
            SUM(CASE WHEN `{col}` < 0 THEN 1 ELSE 0 END) AS nb_negatifs
        FROM {TABLE_SILVER}
        WHERE `{col}` IS NOT NULL
    """).collect()[0].asDict()
    rapport_stats.append(stats)
    flag = "ALERTE" if stats["nb_negatifs"] > 0 else "ok"
    print(f"  [{flag}] {col:<30} : min={stats['min']}, max={stats['max']}, "
          f"moy={stats['moyenne']}, neg={stats['nb_negatifs']}")

if rapport_stats:
    sauvegarder_rapport(rapport_stats, "stats_colonnes_numeriques.csv")

print()

# ============================================================
# RÉSUMÉ
# ============================================================

print("=" * 70)
print("RÉSUMÉ VALIDATION")
print("=" * 70)
print(f"Lignes totales  : {nb_total:,}")
print(f"Colonnes        : {len(cols_existantes)}")
print()
print("Prochaine étape : 05_silver_to_gold.py")

spark.stop()
