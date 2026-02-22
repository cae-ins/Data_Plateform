/* ============================================================
   EXPLORATION — REQUÊTAGE DEPUIS STATA
   ============================================================
   Équivalent Stata de exploration.ipynb
   Deux approches pour lire les tables de la Data Platform :
     A. Télécharger depuis MinIO (staging / exports Gold)
     B. Requêter Trino via ODBC (accès direct aux tables Iceberg)

   Prérequis :
     Approche A : Stata 16+, pip install minio pyarrow pandas
     Approche B : Pilote ODBC Trino installé sur la machine
   ============================================================ */


/* --- CONFIGURATION --- */

/* DEPUIS SA MACHINE LOCALE */
global MINIO_ENDPOINT   "http://192.168.1.230:30137"
global MINIO_ACCESS_KEY "datalab-team"
global MINIO_SECRET_KEY "minio-datalabteam123"

/* DEPUIS JHUB */
/* global MINIO_ENDPOINT   "http://minio.mon-namespace.svc.cluster.local:80" */
/* global MINIO_ACCESS_KEY "datalab-team"                                     */
/* global MINIO_SECRET_KEY "minio-datalabteam123"                             */


/* ============================================================
   OPTION A : Télécharger un export depuis MinIO
   ============================================================
   Cas typique : un export Gold/CSV déposé par Spark ou par R/Python.
   ============================================================ */

/* <<< À MODIFIER >>> */
global BUCKET "staging"
global OBJET  "exports/mon_resultat.parquet"
/* <<<----------->>> */

global FICHIER_TMP "`c(tmpdir)'/donnees_platform.dta"

python:
import io, os
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio

minio_ep   = Stata.macro["MINIO_ENDPOINT"].replace("http://", "")
access_key = Stata.macro["MINIO_ACCESS_KEY"]
secret_key = Stata.macro["MINIO_SECRET_KEY"]
bucket     = Stata.macro["BUCKET"]
objet      = Stata.macro["OBJET"]
dest_dta   = Stata.macro["FICHIER_TMP"]

client   = Minio(endpoint=minio_ep, access_key=access_key,
                 secret_key=secret_key, secure=False)
response = client.get_object(bucket, objet)
contenu  = response.read()
response.close()

# Parquet → pandas → Stata
if objet.endswith(".parquet"):
    import io
    df = pq.read_table(io.BytesIO(contenu)).to_pandas()
elif objet.endswith(".csv"):
    df = pd.read_csv(io.BytesIO(contenu), sep=";", encoding="utf-8-sig")

# Sauvegarder en .dta pour Stata
import pyreadstat
pyreadstat.write_dta(df, dest_dta, version=15)
print(f"✓ {len(df):,} lignes × {df.shape[1]} colonnes chargées depuis {bucket}/{objet}")
end

use "$FICHIER_TMP", clear
display "✓ `c(N)' observations × `c(k)' variables"


/* ============================================================
   ON EFFECTUE LES ANALYSES ICI
   ============================================================ */

/* Statistiques descriptives */
/* summarize <<variable>> */
/* tabulate <<variable>> */
/* histogram <<variable>>, normal */

/* Modèle */
/* regress <<y>> <<x1>> <<x2>> */
/* logit <<y>> <<x1>> <<x2>> */


/* ============================================================ */


/* ============================================================
   OPTION B : Requête Trino via ODBC (accès direct Iceberg)
   ============================================================
   Nécessite le pilote ODBC Trino configuré sur la machine.
   Permet d'interroger les tables Silver/Gold sans télécharger.

   odbc load, exec("SELECT * FROM silver.ma_table LIMIT 1000") ///
              dsn("Trino_ANSTAT") clear
   ============================================================ */
