"""
TEMPLATE : Écriture d'un DataFrame en Parquet sur MinIO (Python)
================================================================
Usage    : Sauvegarder le résultat d'une analyse sur MinIO au format Parquet.
Cas type : Sorties de pipeline (panel_admin, ENE poids, nowcasting...).

Format Parquet recommandé : compression snappy (bon équilibre taille/vitesse).

Dépendances :
    pip install pyarrow s3fs python-dotenv pandas
"""

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv

load_dotenv()

# --- CONNEXION S3/MINIO ---
fs = s3fs.S3FileSystem(
    key          = os.getenv("MINIO_ACCESS_KEY"),
    secret       = os.getenv("MINIO_SECRET_KEY"),
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://192.168.1.230:30137"),
    use_ssl      = False,
)

# ================================================================
# <<< À MODIFIER >>>
BUCKET = "staging"
OBJET  = "resultats/mon_analyse.parquet"
# <<<----------->>>
# ================================================================

# --- VOTRE DATAFRAME ICI ---
# df = ...  # Votre DataFrame pandas

# --- ÉCRITURE FICHIER UNIQUE ---
table = pa.Table.from_pandas(df, preserve_index=False)
with fs.open(f"{BUCKET}/{OBJET}", "wb") as f:
    pq.write_table(table, f, compression="snappy")

print(f"✓ Écrit : s3a://{BUCKET}/{OBJET}  ({len(df):,} lignes)")

# --- OPTION : Écriture partitionnée (ex. par annee) ---
# Utile pour les grosses tables consultées par filtre temporel
#
# pq.write_to_dataset(
#     table,
#     root_path      = f"{BUCKET}/resultats/mon_analyse/",
#     partition_cols = ["annee"],
#     filesystem     = fs,
#     compression    = "snappy",
# )
# Résultat : staging/resultats/mon_analyse/annee=2023/part-0.parquet
