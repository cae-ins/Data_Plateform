# ============================================================
# CHARGEMENT DU STAGING VERS LE BRONZE (R / sparklyr)
# ============================================================
# Équivalent R de staging-bronze.ipynb
# Lit un fichier depuis le bucket MinIO staging
# et l'écrit comme table Iceberg dans le Bronze.
# ============================================================

library(sparklyr)
library(dplyr)
library(arrow)
library(aws.s3)

# --- CONFIGURATION ---

#LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   <- "http://192.168.1.230:30137"
MINIO_ACCESS_KEY <- "datalab-team"
MINIO_SECRET_KEY <- "minio-datalabteam123"
NESSIE_URI       <- "http://192.168.1.230:30604/api/v1"

#---------------------------------------------------------------------------------

#LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   <- "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY <- "datalab-team"
# MINIO_SECRET_KEY <- "minio-datalabteam123"
# NESSIE_URI       <- "http://nessie.trino.svc.cluster.local:19120/api/v1"

#---------------------------------------------------------------------------------

config <- spark_config()
config$spark.driver.memory <- "16g"
config$spark.jars.packages <- paste(
  "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
  "org.apache.hadoop:hadoop-aws:3.3.4",
  "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
  sep = ","
)
config$spark.sql.extensions <- paste(
  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
  "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
  sep = ","
)
config$spark.sql.catalog.nessie                 <- "org.apache.iceberg.spark.SparkCatalog"
config$`spark.sql.catalog.nessie.catalog-impl`  <- "org.apache.iceberg.nessie.NessieCatalog"
config$spark.sql.catalog.nessie.uri             <- NESSIE_URI
config$spark.sql.catalog.nessie.ref             <- "main"
config$spark.sql.catalog.nessie.warehouse       <- "s3a://bronze/"
config$spark.hadoop.fs.s3a.endpoint                   <- MINIO_ENDPOINT
config$spark.hadoop.fs.s3a.access.key                 <- MINIO_ACCESS_KEY
config$spark.hadoop.fs.s3a.secret.key                 <- MINIO_SECRET_KEY
config$spark.hadoop.fs.s3a.path.style.access          <- "true"
config$spark.hadoop.fs.s3a.impl                       <- "org.apache.hadoop.fs.s3a.S3AFileSystem"
config$`spark.hadoop.fs.s3a.aws.credentials.provider` <- "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

sc <- spark_connect(master = "local", config = config)

# ============================================================
# LECTURE DU FICHIER DEPUIS MINIO STAGING
# ============================================================

# <<< À MODIFIER >>>
BUCKET <- "staging"
OBJET  <- "mon_dossier/mon_fichier.<<ext>>"
# <<<----------->>>

endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

# Téléchargement en fichier temporaire
ext  <- tools::file_ext(OBJET)
tmp  <- tempfile(fileext = paste0(".", ext))

aws.s3::save_object(
  object   = OBJET,
  bucket   = BUCKET,
  file     = tmp,
  base_url = endpoint_propre,
  region   = ""
)

# Lecture selon le format
if (ext == "parquet") {
  df_r <- arrow::read_parquet(tmp)
} else if (ext %in% c("xlsx", "xls")) {
  df_r <- readxl::read_excel(tmp, sheet = 1)
} else if (ext == "dta") {
  df_r <- haven::read_dta(tmp)
} else {
  df_r <- data.table::fread(tmp)
}
unlink(tmp)

names(df_r) <- gsub("[ .]", "_", names(df_r))
cat(sprintf("✓ Staging lu : %s lignes × %d colonnes\n",
            format(nrow(df_r), big.mark = ","), ncol(df_r)))

df_spark <- copy_to(sc, df_r, overwrite = TRUE)

# ============================================================
# ON EFFECTUE LES TRAITEMENTS PERSONNALISÉS ICI
# ============================================================



# ============================================================

TABLE_CIBLE <- "nessie.bronze.<<nom_de_la_table>>"

DBI::dbExecute(sc, "CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

spark_write_table(df_spark, TABLE_CIBLE, mode = "overwrite", format = "iceberg")

cat(sprintf("✓ Table écrite : %s\n", TABLE_CIBLE))

spark_disconnect(sc)
