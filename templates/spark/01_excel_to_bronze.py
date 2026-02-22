"""
TEMPLATE SPARK : Excel (MinIO/staging) → Iceberg Bronze
========================================================
Usage    : Ingérer un fichier Excel depuis staging et l'écrire
           comme table Iceberg dans le catalogue Nessie (couche Bronze).
Cas type : panel_admin mensuel, fichiers de solde, données brutes.

Ce script est minimaliste par conception :
la configuration Spark (credentials, packages, mémoire) est injectée
par le YAML Spark Operator — ne pas redéfinir SparkConf ici.

À uploader sur MinIO : s3a://sparkapplication/excel_to_bronze.py
"""

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ================================================================
# <<< À MODIFIER >>>
PATH_SOURCE  = "s3a://staging/mon_dossier/mon_fichier.xlsx"
TABLE_CIBLE  = "nessie.bronze.ma_table"   # nessie.<couche>.<nom_table>
SHEET        = "0"   # "0" = 1ère feuille | ou nom : "Feuil1"
MODE         = "overwrite"  # "overwrite" ou "append"
# <<<----------->>>
# ================================================================

# --- LECTURE EXCEL ---
df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header",      "true")
    .option("inferSchema", "false")  # Tout en String pour Bronze — pas de perte de données
    .option("sheetName",   SHEET)
    .option("dataAddress", f"'{SHEET}'!A1")
    .load(PATH_SOURCE)
)

print(f"✓ Lu : {df.count():,} lignes × {len(df.columns)} colonnes depuis {PATH_SOURCE}")

# ================================================================
# --- TRANSFORMATIONS PRÉ-BRONZE (optionnel) ---
# Ne faire ici que le strict minimum : renommage, cast basique.
# Les transformations métier vont en Silver.

# Exemple : nettoyer les espaces dans les noms de colonnes
for col_name in df.columns:
    propre = col_name.strip().replace(" ", "_").replace(".", "_")
    if propre != col_name:
        df = df.withColumnRenamed(col_name, propre)

# ================================================================

# --- ÉCRITURE ICEBERG BRONZE ---
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

df.write \
    .format("iceberg") \
    .mode(MODE) \
    .saveAsTable(TABLE_CIBLE)

print(f"✓ Table écrite : {TABLE_CIBLE}")

spark.stop()
