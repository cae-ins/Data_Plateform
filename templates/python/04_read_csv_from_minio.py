"""
TEMPLATE : Lecture d'un fichier CSV depuis MinIO (Python)
=========================================================
Usage    : Charger un fichier CSV depuis MinIO.
Cas type : Exports R (fwrite), exports SQL Trino, rapports de validation.

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
OBJET     = "mon_dossier/mon_fichier.csv"
SEPARATOR = ";"       # Séparateur : ";" (R fwrite), "," (Python), "\t" (tabulation)
ENCODING  = "utf-8"   # "utf-8", "latin-1" (fichiers Stata/SPSS français), "utf-8-sig"
# <<<----------->>>
# ================================================================

# --- LECTURE ---
response = client.get_object(BUCKET, OBJET)
df = pd.read_csv(
    io.BytesIO(response.read()),
    sep      = SEPARATOR,
    encoding = ENCODING,
    low_memory = False,   # Évite les warnings de type inference sur gros fichiers
)
response.close()
response.release_conn()

print(f"✓ {len(df):,} lignes × {df.shape[1]} colonnes chargées depuis {BUCKET}/{OBJET}")
print(df.head())

# ================================================================
# --- VOS TRAITEMENTS ICI ---

# ================================================================
