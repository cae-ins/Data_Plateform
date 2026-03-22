/* ============================================================
   CHARGEMENT DU STAGING VERS LE BRONZE (Stata)
   ============================================================
   Équivalent Stata de staging-bronze.ipynb
   Télécharge un fichier depuis MinIO staging,
   effectue les traitements Stata, puis redépose sur staging
   pour ingestion Bronze via job Spark.

   Prérequis : Stata 16+ (pour les blocs python:)
               pip install minio pyarrow pandas pyreadstat
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


/* --- PARAMÈTRES FICHIER --- */
/* <<< À MODIFIER >>> */
global BUCKET_SOURCE "staging"
global OBJET_SOURCE  "mon_dossier/mon_fichier.dta"
/* <<< ----------- >>> */

global FICHIER_TMP "`c(tmpdir)'/fichier_depuis_minio.dta"


/* ============================================================
   TÉLÉCHARGEMENT DEPUIS MINIO (via Python intégré)
   ============================================================ */

python:
import os
from minio import Minio

minio_ep   = Stata.macro["MINIO_ENDPOINT"].replace("http://", "")
access_key = Stata.macro["MINIO_ACCESS_KEY"]
secret_key = Stata.macro["MINIO_SECRET_KEY"]
bucket     = Stata.macro["BUCKET_SOURCE"]
objet      = Stata.macro["OBJET_SOURCE"]
dest       = Stata.macro["FICHIER_TMP"]

client = Minio(endpoint=minio_ep, access_key=access_key,
               secret_key=secret_key, secure=False)
client.fget_object(bucket, objet, dest)
print(f"✓ Téléchargé : s3://{bucket}/{objet} → {dest}")
end


/* ============================================================
   LECTURE DU FICHIER EN STATA
   ============================================================ */

/* Option .dta */
use "$FICHIER_TMP", clear

/* Option Excel (si l'objet est un .xlsx) */
/* import excel using "$FICHIER_TMP", firstrow clear */

/* Option CSV */
/* import delimited "$FICHIER_TMP", delimiter(";") clear */

/* Option Parquet — passer par Python pour convertir en .dta d'abord */
/* (voir bloc python: ci-dessous) */

display "✓ `c(N)' observations × `c(k)' variables chargées"


/* ============================================================
   ON EFFECTUE LES TRAITEMENTS PERSONNALISÉS ICI
   ============================================================ */




/* ============================================================ */


/* --- REDÉPÔT SUR MINIO (résultat traité) --- */

local fichier_result = "`c(tmpdir)'/resultat_stata.csv"
export delimited "`fichier_result'", delimiter(";") replace

python:
import io, os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

minio_ep   = Stata.macro["MINIO_ENDPOINT"].replace("http://", "")
access_key = Stata.macro["MINIO_ACCESS_KEY"]
secret_key = Stata.macro["MINIO_SECRET_KEY"]
src        = Stata.macro["fichier_result"]

df    = pd.read_csv(src, sep=";", encoding="utf-8-sig")
table = pa.Table.from_pandas(df, preserve_index=False)
buf   = io.BytesIO()
pq.write_table(table, buf, compression="snappy")
buf.seek(0)

client = Minio(endpoint=minio_ep, access_key=access_key,
               secret_key=secret_key, secure=False)

# <<< À MODIFIER >>>
BUCKET_DEST = "staging"
OBJET_DEST  = "<<dossier>>/<<nom_resultat>>.parquet"
# <<<----------->>>

client.put_object(BUCKET_DEST, OBJET_DEST, buf, length=buf.getbuffer().nbytes,
                  content_type="application/octet-stream")
print(f"✓ Résultat déposé : s3://{BUCKET_DEST}/{OBJET_DEST}")
os.remove(src)
os.remove(Stata.macro["FICHIER_TMP"])
end

display "✓ Traitement terminé — lancer ensuite le job Spark pour ingérer vers Bronze"
