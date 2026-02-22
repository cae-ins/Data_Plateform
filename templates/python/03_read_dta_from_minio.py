"""
TEMPLATE : Lecture d'un fichier Stata (.dta) depuis MinIO (Python)
==================================================================
Usage    : Charger un fichier Stata depuis MinIO (pipelines CNPS, CAE-IPM, ENE).
           Conserve les labels de variables Stata dans les métadonnées.

Dépendances :
    pip install minio python-dotenv pyreadstat pandas
"""

import io
import os

import pandas as pd
import pyreadstat
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
BUCKET = "staging"
OBJET  = "cnps/donnees_traitees.dta"   # ou ENE, CAE-IPM...
# <<<----------->>>
# ================================================================

# --- TÉLÉCHARGEMENT EN MÉMOIRE ---
response = client.get_object(BUCKET, OBJET)
contenu  = response.read()
response.close()
response.release_conn()

# --- LECTURE STATA ---
# pyreadstat préserve les value labels, column labels et formats Stata
df, meta = pyreadstat.read_dta(io.BytesIO(contenu))

print(f"✓ {len(df):,} lignes × {df.shape[1]} colonnes")
print(f"  Stata version : {meta.file_label}")
print(f"  Labels disponibles : {list(meta.column_labels.items())[:5]} ...")

# Accès aux value labels (modalités codées)
# meta.value_labels  → dict {variable: {code: label}}
# ex : meta.value_labels.get("sexe") → {1: "Masculin", 2: "Féminin"}

# --- OPTION : Appliquer les value labels aux colonnes ---
# df_labelled = pyreadstat.set_value_labels(df, meta, formats_as_category=True)

# ================================================================
# --- VOS TRAITEMENTS ICI ---

# ================================================================
