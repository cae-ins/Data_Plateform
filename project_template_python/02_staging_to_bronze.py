# ============================================================
# MON_PROJET — ÉTAPE 2 : STAGING → BRONZE ICEBERG
# ============================================================
# Lit chaque fichier source depuis MinIO staging,
# normalise les noms de colonnes (sans transformation métier)
# et ingère vers la table Bronze Iceberg via PySpark.
#
# Bronze = données brutes, tout en StringType, fichiers empilés.
# Les transformations métier se font en Silver (étape 3).
#
# Table produite : nessie.bronze.mon_projet
#
# Dépendances :
#   pip install boto3 pandas openpyxl python-dotenv pyspark
# ============================================================

import os
import re
import tempfile
import unicodedata
import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

load_dotenv()

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")
NESSIE_URI       = os.getenv("NESSIE_URI",       "http://192.168.1.230:30604/api/v1")

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# NESSIE_URI       = "http://nessie.trino.svc.cluster.local:19120/api/v1"

# ============================================================
# <<< À MODIFIER >>>
BUCKET          = "staging"
PREFIX_SOURCES  = "mon_projet/sources"       # fichiers à ingérer
TABLE_BRONZE    = "nessie.bronze.mon_projet"  # table Iceberg cible
TAILLE_LOT      = 10                          # fichiers par batch

# Paramètre lecture selon le type de fichier
SHEET_EXCEL     = 0       # index feuille Excel
SEPARATEUR_CSV  = ","
ENCODAGE        = "utf-8"
# <<<----------->>>
# ============================================================


# ============================================================
# UTILITAIRES
# ============================================================

def normaliser_nom_colonne(nom: str) -> str:
    """Normalise un nom de colonne : ASCII majuscule, underscores."""
    if not nom or pd.isna(nom):
        return "colonne_vide"
    nom = unicodedata.normalize("NFKD", str(nom))
    nom = nom.encode("ascii", "ignore").decode()
    nom = nom.upper().strip()
    nom = re.sub(r"[\s\-/.]", "_", nom)
    nom = re.sub(r"[^A-Z0-9_]", "", nom)
    nom = re.sub(r"_+", "_", nom).strip("_")
    return nom or "colonne_vide"


def deduplicer_colonnes(colonnes: list) -> list:
    """Ajoute un suffixe numérique aux doublons."""
    vus = {}
    resultat = []
    for col in colonnes:
        if col in vus:
            vus[col] += 1
            resultat.append(f"{col}_{vus[col]}")
        else:
            vus[col] = 0
            resultat.append(col)
    return resultat


def lire_fichier(chemin: str) -> pd.DataFrame:
    """Lit un fichier local en DataFrame pandas (tout en str)."""
    ext = chemin.lower().rsplit(".", 1)[-1]
    if ext in ("xlsx", "xls"):
        return pd.read_excel(chemin, sheet_name=SHEET_EXCEL, dtype=str)
    elif ext == "parquet":
        return pd.read_parquet(chemin).astype(str)
    elif ext == "csv":
        return pd.read_csv(chemin, dtype=str, sep=SEPARATEUR_CSV, encoding=ENCODAGE)
    else:
        raise ValueError(f"Format non supporté : {ext}")


# ============================================================
# CONNEXION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("staging_to_bronze")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

# --- CONNEXION MINIO ---

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    region_name           = "us-east-1",
    verify                = False,
)

# ============================================================
# LISTER LES FICHIERS DANS STAGING
# ============================================================

print("=" * 70)
print(f"STAGING → BRONZE : {TABLE_BRONZE}")
print("=" * 70)

paginator = s3.get_paginator("list_objects_v2")
pages     = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_SOURCES)
fichiers  = sorted(obj["Key"] for page in pages for obj in page.get("Contents", []))

print(f"Fichiers dans staging : {len(fichiers)}")
print(f"Taille des lots       : {TAILLE_LOT}")

lots = [fichiers[i:i+TAILLE_LOT] for i in range(0, len(fichiers), TAILLE_LOT)]
print(f"Nombre de lots        : {len(lots)}\n")

# ============================================================
# TRAITEMENT PAR LOTS
# ============================================================

premier_lot = True

for i_lot, lot in enumerate(lots, 1):
    print(f"[LOT {i_lot}/{len(lots)}] {len(lot)} fichiers")
    print("-" * 70)

    frames = []

    for cle in lot:
        nom = cle.split("/")[-1]
        ext = nom.lower().rsplit(".", 1)[-1]
        print(f"  {nom} ... ", end="", flush=True)

        try:
            with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
                s3.download_file(BUCKET, cle, tmp.name)
                df = lire_fichier(tmp.name)

            # Normaliser les colonnes
            noms_norm = [normaliser_nom_colonne(c) for c in df.columns]
            noms_norm = deduplicer_colonnes(noms_norm)
            df.columns = noms_norm

            # Tout en str (Bronze = raw)
            df = df.astype(str).replace("nan", None)

            # Métadonnées
            df["_FICHIER_SOURCE"] = nom

            frames.append(df)
            print(f"ok — {len(df):,} lignes × {len(df.columns)} cols")

        except Exception as e:
            print(f"ERREUR : {e}")

    if not frames:
        print(f"  Lot {i_lot} vide, passage au suivant\n")
        continue

    # Consolider le lot (union avec toutes les colonnes)
    toutes_cols = sorted(set(c for df in frames for c in df.columns))
    frames = [df.reindex(columns=toutes_cols) for df in frames]
    base_lot = pd.concat(frames, ignore_index=True).astype(str).replace("nan", None)

    print(f"\n  Lot {i_lot} : {len(base_lot):,} lignes × {len(base_lot.columns)} colonnes")

    # Écriture vers Bronze Iceberg
    print(f"  Écriture vers {TABLE_BRONZE} ... ", end="", flush=True)
    df_spark = spark.createDataFrame(base_lot)
    # Cast explicite en StringType
    for col in df_spark.columns:
        df_spark = df_spark.withColumn(col, F.col(col).cast(StringType()))

    mode = "overwrite" if premier_lot else "append"
    df_spark.writeTo(TABLE_BRONZE).using("iceberg").tableProperty("format-version", "2").createOrReplace() \
        if premier_lot else df_spark.writeTo(TABLE_BRONZE).append()

    print("ok\n")
    premier_lot = False

# ============================================================
# RÉSUMÉ
# ============================================================

print("=" * 70)
nb_total = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE_BRONZE}").collect()[0]["n"]
print(f"Table Bronze : {nb_total:,} lignes")
print(f"Table        : {TABLE_BRONZE}")
print("\nProchaine étape : 03_bronze_to_silver.py")

spark.stop()
