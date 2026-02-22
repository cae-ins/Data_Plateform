"""
TEMPLATE : Lecture d'un fichier Excel depuis MinIO (Python)
===========================================================
Usage    : Charger un fichier Excel (.xlsx) stocké sur MinIO dans un DataFrame pandas.
Cas type : Fichiers bruts déposés dans le bucket staging (panel_admin, CNPS, ENE...).

Dépendances :
    pip install minio python-dotenv pandas openpyxl
"""

import io
import os

import pandas as pd
from dotenv import load_dotenv
from minio import Minio

load_dotenv()  # Charge le fichier .env à la racine du projet

# --- CONNEXION MINIO ---
client = Minio(
    endpoint   = os.getenv("MINIO_ENDPOINT", "192.168.1.230:30137").replace("http://", "").replace("https://", ""),
    access_key = os.getenv("MINIO_ACCESS_KEY"),
    secret_key = os.getenv("MINIO_SECRET_KEY"),
    secure     = False,  # Mettre True si endpoint HTTPS
)

# ================================================================
# <<< À MODIFIER >>>
BUCKET = "staging"
OBJET  = "mon_dossier/mon_fichier.xlsx"   # Chemin dans le bucket
SHEET  = 0     # 0 = première feuille | ou nom de feuille : "Feuil1"
# <<<----------->>>
# ================================================================

# --- LECTURE ---
response = client.get_object(BUCKET, OBJET)
df = pd.read_excel(io.BytesIO(response.read()), sheet_name=SHEET)
response.close()
response.release_conn()

print(f"✓ {len(df):,} lignes × {df.shape[1]} colonnes chargées depuis {BUCKET}/{OBJET}")
print(df.head())

# ================================================================
# --- VOS TRAITEMENTS ICI ---

# ================================================================
