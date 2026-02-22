"""
TEMPLATE : Écriture d'un DataFrame en CSV sur MinIO (Python)
============================================================
Usage    : Exporter des résultats au format CSV (lisible par Excel, R, Stata).
           Utile quand le destinataire n'a pas accès à Trino/Parquet.

Dépendances :
    pip install minio python-dotenv pandas
"""

import io
import os

import pandas as pd
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# --- CONNEXION MINIO ---
client = Minio(
    endpoint   = os.getenv("MINIO_ENDPOINT", "192.168.1.230:30137").replace("http://", "").replace("https://", ""),
    access_key = os.getenv("MINIO_ACCESS_KEY"),
    secret_key = os.getenv("MINIO_SECRET_KEY"),
    secure     = False,
)

# ================================================================
# <<< À MODIFIER >>>
BUCKET    = "staging"
OBJET     = "resultats/mon_analyse.csv"
SEPARATOR = ";"          # ";" recommandé pour Excel en français
ENCODING  = "utf-8-sig"  # utf-8-sig inclut le BOM → Excel ouvre correctement
# <<<----------->>>
# ================================================================

# --- VOTRE DATAFRAME ICI ---
# df = ...

# --- ÉCRITURE EN MÉMOIRE PUIS UPLOAD ---
buffer = io.BytesIO()
df.to_csv(buffer, index=False, sep=SEPARATOR, encoding=ENCODING)
buffer.seek(0)
taille = buffer.getbuffer().nbytes

client.put_object(
    bucket_name  = BUCKET,
    object_name  = OBJET,
    data         = buffer,
    length       = taille,
    content_type = "text/csv",
)

print(f"✓ Écrit : s3://{BUCKET}/{OBJET}  ({taille / 1024:.1f} Ko, {len(df):,} lignes)")
