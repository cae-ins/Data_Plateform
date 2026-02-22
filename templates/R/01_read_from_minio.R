# ============================================================
# TEMPLATE : Lecture de données depuis MinIO (R)
# ============================================================
# Usage    : Charger des données depuis MinIO directement depuis R.
# Cas type : panel_admin (Parquet), ENE (CSV/Parquet), CAE-IPM, CNPS.
#
# Dépendances :
#   install.packages(c("arrow", "aws.s3", "dotenv", "data.table"))
#   Pour Stata : install.packages("haven")
#   Pour Excel : install.packages("readxl")
# ============================================================

library(arrow)
library(dotenv)

# --- CONFIGURATION ---
# Charger les credentials depuis .env à la racine du projet
load_dot_env(file = ".env")

MINIO_ENDPOINT   <- Sys.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY <- Sys.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY <- Sys.getenv("MINIO_SECRET_KEY")

# Nettoyer l'endpoint (arrow attend host:port sans http://)
endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)


# ============================================================
# OPTION 1 : PARQUET (via arrow — streaming direct, pas de téléchargement)
# ============================================================
# Recommandé pour : gros fichiers, bases panel, résultats pipelines R
# ============================================================

s3 <- S3FileSystem$create(
  access_key        = MINIO_ACCESS_KEY,
  secret_key        = MINIO_SECRET_KEY,
  endpoint_override = endpoint_propre,
  scheme            = "http",    # "https" si endpoint sécurisé
  region            = "us-east-1"  # Requis syntaxiquement, ignoré par MinIO
)

# <<< À MODIFIER >>>
BUCKET <- "staging"
OBJET  <- "mon_dossier/ma_table.parquet"
# <<<----------->>>

df <- read_parquet(s3$path(paste0(BUCKET, "/", OBJET)))
cat(sprintf("✓ %s lignes × %d colonnes chargées (Parquet)\n",
            format(nrow(df), big.mark = ","), ncol(df)))

# Lecture d'un DOSSIER Parquet partitionné :
# df <- open_dataset(s3$path(paste0(BUCKET, "/mon_dossier/"))) |> collect()


# ============================================================
# OPTION 2 : CSV (téléchargement temporaire via aws.s3)
# ============================================================
# Recommandé pour : petits fichiers, exports R fwrite, rapports validation
# ============================================================

library(aws.s3)
library(data.table)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

# <<< À MODIFIER >>>
BUCKET_CSV <- "staging"
OBJET_CSV  <- "mon_dossier/mon_fichier.csv"
# <<<----------->>>

tmp <- tempfile(fileext = ".csv")
aws.s3::save_object(
  object   = OBJET_CSV,
  bucket   = BUCKET_CSV,
  file     = tmp,
  base_url = endpoint_propre,
  region   = ""
)
df_csv <- fread(tmp, encoding = "UTF-8")
unlink(tmp)

cat(sprintf("✓ %s lignes chargées (CSV)\n", format(nrow(df_csv), big.mark = ",")))


# ============================================================
# OPTION 3 : STATA .dta (téléchargement temporaire + haven)
# ============================================================
# Recommandé pour : données CNPS, CAE-IPM, ENE traitées sous Stata
# ============================================================

# library(haven)
#
# OBJET_DTA <- "cnps/donnees_traitees.dta"
# tmp_dta   <- tempfile(fileext = ".dta")
#
# aws.s3::save_object(
#   object   = OBJET_DTA,
#   bucket   = "staging",
#   file     = tmp_dta,
#   base_url = endpoint_propre,
#   region   = ""
# )
# df_dta <- haven::read_dta(tmp_dta)
# unlink(tmp_dta)
#
# # Les labels Stata sont accessibles via :
# attr(df_dta$sexe, "labels")        # Value labels
# attr(df_dta$sexe, "label")         # Variable label


# ============================================================
# OPTION 4 : EXCEL (téléchargement temporaire + readxl)
# ============================================================

# library(readxl)
#
# OBJET_XLSX <- "panel_admin/2023_01.xlsx"
# tmp_xlsx   <- tempfile(fileext = ".xlsx")
#
# aws.s3::save_object(
#   object   = OBJET_XLSX,
#   bucket   = "staging",
#   file     = tmp_xlsx,
#   base_url = endpoint_propre,
#   region   = ""
# )
# df_xl <- read_excel(tmp_xlsx, sheet = 1)
# unlink(tmp_xlsx)


# ============================================================
# --- VOS TRAITEMENTS ICI ---

# ============================================================
