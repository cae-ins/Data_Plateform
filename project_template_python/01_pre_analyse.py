# ============================================================
# MON_PROJET — ÉTAPE 1 : PRÉ-ANALYSE DES FICHIERS STAGING
# ============================================================
# Liste les fichiers présents dans staging, inspecte leurs
# colonnes/dimensions et sauvegarde un rapport CSV sur MinIO.
#
# Utile avant l'ingestion pour anticiper les transformations
# nécessaires (colonnes manquantes, formats, encodages...).
#
# Dépendances :
#   pip install boto3 pandas openpyxl python-dotenv
# ============================================================

import io
import os
import csv
import tempfile
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

# ============================================================
# <<< À MODIFIER >>>
BUCKET          = "staging"
PREFIX_SOURCES  = "mon_projet/sources"    # où sont les fichiers sources
PREFIX_SORTIE   = "mon_projet/pre_analyse" # où écrire les rapports

# Paramètre lecture selon le type de fichier
SHEET_EXCEL     = 0       # index feuille Excel (0 = première)
SEPARATEUR_CSV  = ","     # séparateur CSV
ENCODAGE        = "utf-8" # encodage par défaut
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

# --- LISTER LES FICHIERS ---

print("=" * 70)
print("PRÉ-ANALYSE : Fichiers dans staging")
print("=" * 70)

paginator = s3.get_paginator("list_objects_v2")
pages     = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_SOURCES)
objets    = [obj for page in pages for obj in page.get("Contents", [])]
fichiers  = sorted(obj["Key"] for obj in objets)

print(f"{len(fichiers)} fichiers trouvés dans s3://{BUCKET}/{PREFIX_SOURCES}\n")


def lire_entetes(s3_client, bucket, cle):
    """Télécharge un fichier et retourne (colonnes, nb_lignes, nb_cols)."""
    ext = cle.lower().rsplit(".", 1)[-1]
    with tempfile.NamedTemporaryFile(suffix=f".{ext}") as tmp:
        s3_client.download_file(bucket, cle, tmp.name)
        if ext in ("xlsx", "xls"):
            df = pd.read_excel(tmp.name, sheet_name=SHEET_EXCEL, nrows=0)
        elif ext == "parquet":
            df = pd.read_parquet(tmp.name).iloc[:0]
        elif ext == "csv":
            df = pd.read_csv(tmp.name, nrows=0, sep=SEPARATEUR_CSV, encoding=ENCODAGE)
        else:
            return None, None, None

        # Pour avoir nb_lignes on relit juste le compte
        if ext in ("xlsx", "xls"):
            df_full = pd.read_excel(tmp.name, sheet_name=SHEET_EXCEL, usecols=[0])
        elif ext == "parquet":
            df_full = pd.read_parquet(tmp.name, columns=[df.columns[0]])
        else:
            df_full = pd.read_csv(tmp.name, usecols=[0], sep=SEPARATEUR_CSV, encoding=ENCODAGE)

        return list(df.columns), len(df_full), len(df.columns)


# --- INSPECTION ---

rapport = []
toutes_colonnes = set()
erreurs = []

for cle in fichiers:
    nom = cle.split("/")[-1]
    print(f"  {nom} ... ", end="", flush=True)
    try:
        colonnes, nb_lignes, nb_cols = lire_entetes(s3, BUCKET, cle)
        if colonnes is None:
            print("format non supporté")
            continue
        toutes_colonnes.update(colonnes)
        rapport.append({
            "fichier": nom,
            "cle":     cle,
            "nb_lignes": nb_lignes,
            "nb_colonnes": nb_cols,
            "colonnes": "|".join(colonnes),
        })
        print(f"ok — {nb_lignes:,} lignes × {nb_cols} cols")
    except Exception as e:
        print(f"ERREUR : {e}")
        erreurs.append({"fichier": nom, "erreur": str(e)})

print(f"\n{len(rapport)} fichiers analysés, {len(erreurs)} erreurs")
print(f"Colonnes uniques (toutes sources) : {len(toutes_colonnes)}")


# --- SAUVEGARDE RAPPORTS ---

def sauvegarder_csv(donnees, nom_fichier):
    """Sauvegarde une liste de dicts en CSV vers MinIO."""
    if not donnees:
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=donnees[0].keys())
    writer.writeheader()
    writer.writerows(donnees)
    body = buf.getvalue().encode("utf-8")
    cle  = f"{PREFIX_SORTIE}/{nom_fichier}"
    s3.put_object(Bucket=BUCKET, Key=cle, Body=body, ContentType="text/csv")
    print(f"  Rapport : s3://{BUCKET}/{cle}")


print("\n--- Sauvegarde des rapports ---")
sauvegarder_csv(rapport, "inventaire_fichiers.csv")
sauvegarder_csv(
    [{"colonne": c} for c in sorted(toutes_colonnes)],
    "colonnes_uniques.csv"
)
if erreurs:
    sauvegarder_csv(erreurs, "erreurs.csv")

print("\nProchaine étape : 02_staging_to_bronze.py")
