# ============================================================
# TEMPLATE : Écriture de données vers MinIO (R)
# ============================================================
# Usage    : Sauvegarder des résultats R sur MinIO.
# Cas type : Bases consolidées panel_admin, poids ENE calibrés,
#            résultats IPM/IAI, exports pour Trino/Spark.
#
# Dépendances :
#   install.packages(c("arrow", "aws.s3", "dotenv", "data.table"))
#   Pour Stata : install.packages("haven")
# ============================================================

library(arrow)
library(dotenv)

# --- CONFIGURATION ---
load_dot_env(file = ".env")

MINIO_ENDPOINT   <- Sys.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY <- Sys.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY <- Sys.getenv("MINIO_SECRET_KEY")

endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)


# ============================================================
# OPTION 1 : PARQUET (via arrow — recommandé)
# ============================================================
# Format de référence de la Data Platform.
# Compatible avec Spark, Trino, Python pyarrow/polars.
# ============================================================

s3 <- S3FileSystem$create(
  access_key        = MINIO_ACCESS_KEY,
  secret_key        = MINIO_SECRET_KEY,
  endpoint_override = endpoint_propre,
  scheme            = "http",
  region            = "us-east-1"
)

# <<< À MODIFIER >>>
BUCKET <- "staging"
OBJET  <- "resultats/mon_analyse.parquet"
# <<<----------->>>

# --- VOTRE DATAFRAME ICI ---
# df <- ...  # data.frame, data.table, tibble — tous compatibles arrow

write_parquet(df, s3$path(paste0(BUCKET, "/", OBJET)), compression = "snappy")
cat(sprintf("✓ Parquet écrit : s3a://%s/%s  (%s lignes)\n",
            BUCKET, OBJET, format(nrow(df), big.mark = ",")))

# Écriture PARTITIONNÉE par colonne (ex. annee) :
# write_dataset(
#   dataset        = df,
#   path           = s3$path(paste0(BUCKET, "/resultats/mon_analyse/")),
#   format         = "parquet",
#   partitioning   = "annee",
#   compression    = "snappy"
# )


# ============================================================
# OPTION 2 : CSV (via aws.s3)
# ============================================================
# Utile pour partager avec des utilisateurs sans accès Trino/Spark.
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
OBJET_CSV  <- "resultats/mon_analyse.csv"
# <<<----------->>>

tmp <- tempfile(fileext = ".csv")
# sep = ";" et bom = TRUE pour compatibilité Excel français
fwrite(df, tmp, sep = ";", bom = TRUE, encoding = "UTF-8")

aws.s3::put_object(
  file     = tmp,
  object   = OBJET_CSV,
  bucket   = BUCKET_CSV,
  base_url = endpoint_propre,
  region   = ""
)
unlink(tmp)

cat(sprintf("✓ CSV écrit : s3://%s/%s\n", BUCKET_CSV, OBJET_CSV))


# ============================================================
# OPTION 3 : STATA .dta (haven → MinIO)
# ============================================================
# Utile pour repartager des données vers les équipes Stata.
# ============================================================

# library(haven)
#
# OBJET_DTA <- "resultats/base_finale.dta"
# tmp_dta   <- tempfile(fileext = ".dta")
#
# haven::write_dta(df, tmp_dta, version = 15)  # version = 13, 14 ou 15
#
# aws.s3::put_object(
#   file     = tmp_dta,
#   object   = OBJET_DTA,
#   bucket   = "staging",
#   base_url = endpoint_propre,
#   region   = ""
# )
# unlink(tmp_dta)
# cat(sprintf("✓ Stata écrit : s3://staging/%s\n", OBJET_DTA))
