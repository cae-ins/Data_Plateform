# ============================================================
# MON_PROJET — ÉTAPE 0 : UPLOAD DES SOURCES VERS MINIO STAGING
# ============================================================
# À exécuter UNE SEULE FOIS pour déposer les fichiers sources
# sur MinIO. Ensuite, toute l'équipe travaille depuis staging.
#
# Dépendances :
#   pip install boto3 python-dotenv
# ============================================================

import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCALE
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"

# ============================================================
# <<< À MODIFIER >>>
DOSSIER_SOURCES = "/chemin/vers/mes/fichiers/sources"   # dossier local à uploader
EXTENSIONS      = [".csv", ".xlsx", ".parquet", ".dta"] # extensions à inclure
BUCKET          = "staging"
PREFIX_STAGING  = "mon_projet/sources"                  # préfixe dans le bucket
# <<<----------->>>
# ============================================================

# --- CONNEXION MINIO ---

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    region_name           = "us-east-1",
    verify                = False,
)

# --- UPLOAD ---

print("=" * 60)
print("UPLOAD : Sources vers MinIO staging")
print("=" * 60)

dossier = Path(DOSSIER_SOURCES)
fichiers = sorted([
    f for f in dossier.rglob("*")
    if f.is_file() and f.suffix.lower() in EXTENSIONS
])

print(f"Fichiers trouvés : {len(fichiers)}\n")

ok, err = 0, 0

for fichier in fichiers:
    chemin_relatif = fichier.relative_to(dossier)
    cle_dest       = f"{PREFIX_STAGING}/{chemin_relatif.as_posix()}"

    print(f"  Upload : {chemin_relatif} ... ", end="", flush=True)
    try:
        s3.upload_file(str(fichier), BUCKET, cle_dest)
        print("ok")
        ok += 1
    except Exception as e:
        print(f"ERREUR : {e}")
        err += 1

print(f"\n{ok} fichiers uploadés, {err} erreurs")
print(f"\nDisponible sur : s3://{BUCKET}/{PREFIX_STAGING}/")
print("\nProchaine étape : 01_pre_analyse.py")
