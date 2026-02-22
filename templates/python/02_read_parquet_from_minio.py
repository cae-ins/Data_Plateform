"""
TEMPLATE : Lecture d'un fichier Parquet depuis MinIO (Python)
=============================================================
Usage    : Charger un fichier ou dossier Parquet depuis MinIO.
Cas type : Résultats d'un pipeline R (arrow), sorties panel_admin, ENE, etc.

Dépendances :
    pip install pyarrow s3fs python-dotenv pandas
    Optionnel (plus rapide sur gros volumes) : pip install polars
"""

import os

import pandas as pd
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
OBJET  = "mon_dossier/ma_table.parquet"
# <<<----------->>>
# ================================================================

# --- OPTION A : Fichier Parquet unique ---
with fs.open(f"{BUCKET}/{OBJET}", "rb") as f:
    table = pq.read_table(f)
df = table.to_pandas()

# --- OPTION B : Dossier Parquet partitionné (ex. partitionné par annee/) ---
# dataset = pq.ParquetDataset(f"{BUCKET}/{OBJET}/", filesystem=fs)
# df = dataset.read().to_pandas()

# --- OPTION C : Polars (plus rapide pour très gros fichiers) ---
# import polars as pl
# with fs.open(f"{BUCKET}/{OBJET}", "rb") as f:
#     df = pl.read_parquet(f)

print(f"✓ {len(df):,} lignes × {df.shape[1]} colonnes chargées depuis {BUCKET}/{OBJET}")
print(df.dtypes)

# ================================================================
# --- VOS TRAITEMENTS ICI ---

# ================================================================
