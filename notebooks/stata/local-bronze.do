/* ============================================================
   CHARGEMENT LOCAL VERS LE BRONZE (Stata)
   ============================================================
   Équivalent Stata de local-bronze.ipynb
   Charge un fichier local et l'écrit dans le Bronze via MinIO.

   Stata ne se connecte pas directement à Iceberg/Nessie.
   Stratégie : Stata fait les traitements, Python (intégré)
   gère le dépôt sur MinIO, puis un job Spark ingère vers Bronze.

   Prérequis : Stata 16+ (pour les blocs python:)
               pip install minio pyarrow pandas
   ============================================================ */


/* --- CONFIGURATION --- */

/* Choisir UN des deux blocs selon l'environnement */

/* DEPUIS SA MACHINE LOCALE */
global MINIO_ENDPOINT   "http://192.168.1.230:30137"
global MINIO_ACCESS_KEY "datalab-team"
global MINIO_SECRET_KEY "minio-datalabteam123"

/*-------------------------------------------------------------------------*/

/* DEPUIS JHUB (décommenter et commenter le bloc ci-dessus) */
/* global MINIO_ENDPOINT   "http://minio.mon-namespace.svc.cluster.local:80" */
/* global MINIO_ACCESS_KEY "datalab-team"                                     */
/* global MINIO_SECRET_KEY "minio-datalabteam123"                             */

/*-------------------------------------------------------------------------*/


/* ============================================================
   LECTURE DU FICHIER LOCAL
   ============================================================ */

/* <<< À MODIFIER : chemin vers votre fichier local >>> */

/* Option Excel */
import excel using "<<CHEMIN_LOCAL>>/mon_fichier.xlsx", ///
    sheet("Feuil1") firstrow clear

/* Option CSV */
/* import delimited "<<CHEMIN_LOCAL>>/mon_fichier.csv", ///
       delimiter(";") encoding("UTF-8") clear */

/* Option Stata .dta */
/* use "<<CHEMIN_LOCAL>>/mon_fichier.dta", clear */


/* ============================================================
   ON EFFECTUE LES TRAITEMENTS PERSONNALISÉS ICI
   ============================================================ */




/* ============================================================ */


/* --- SAUVEGARDE LOCALE TEMPORAIRE (format Parquet via Python) --- */

/* On sauvegarde en CSV puis Python le convertit en Parquet et l'uploade */
local fichier_tmp = "`c(tmpdir)'/export_stata_tmp.csv"
export delimited "`fichier_tmp'", delimiter(";") replace

display "Fichier temporaire : `fichier_tmp'"


/* --- UPLOAD VERS MINIO STAGING (via Python intégré) --- */
/* Le fichier Parquet sera ensuite ingéré vers Bronze par un job Spark */

python:
import sys, os, io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

fichier_tmp  = Stata.macro["fichier_tmp"]
minio_ep     = Stata.macro["MINIO_ENDPOINT"].replace("http://", "")
access_key   = Stata.macro["MINIO_ACCESS_KEY"]
secret_key   = Stata.macro["MINIO_SECRET_KEY"]

# Lire le CSV temporaire
df = pd.read_csv(fichier_tmp, sep=";", encoding="utf-8-sig")
print(f"✓ {len(df):,} lignes × {df.shape[1]} colonnes")

# Convertir en Parquet en mémoire
table  = pa.Table.from_pandas(df, preserve_index=False)
buf    = io.BytesIO()
pq.write_table(table, buf, compression="snappy")
buf.seek(0)

# Upload sur MinIO staging
client = Minio(endpoint=minio_ep, access_key=access_key,
               secret_key=secret_key, secure=False)

# <<< À MODIFIER >>>
BUCKET = "staging"
OBJET  = "<<dossier>>/<<nom_de_la_table>>.parquet"
# <<<----------->>>

client.put_object(BUCKET, OBJET, buf, length=buf.getbuffer().nbytes,
                  content_type="application/octet-stream")
print(f"✓ Déposé sur MinIO : s3://{BUCKET}/{OBJET}")
os.remove(fichier_tmp)
end

display "✓ Fichier sur staging — lancer ensuite le job Spark (parquet_to_bronze.yaml)"
