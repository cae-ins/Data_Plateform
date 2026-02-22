# ============================================================
# AGRÉGATION SILVER → GOLD (R / sparklyr)
# ============================================================
# Équivalent R de silver-gold.ipynb
# Lit une table Silver, produit des agrégations Gold,
# et exporte vers Iceberg Gold et/ou CSV staging.
# ============================================================

library(sparklyr)
library(dplyr)
library(arrow)

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

# Lecture Silver
df <- spark_read_table(sc, "nessie.silver.<<nom_de_la_table>>")
cat(sprintf("Silver : %s lignes\n", format(sdf_nrow(df), big.mark = ",")))

# ============================================================
# ON EFFECTUE LES AGRÉGATIONS GOLD ICI
# ============================================================
# Gold = tables dénormalisées, pré-agrégées pour dashboards.

# --- OPTION A : dplyr ---
df_gold <- df %>%
  group_by(<<dimension_1>>, <<dimension_2>>) %>%
  summarise(
    nb      = n(),
    total   = sum(<<montant>>, na.rm = TRUE),
    moyenne = mean(<<montant>>, na.rm = TRUE),
    mediane = percentile(<<montant>>, 0.5),
    .groups = "drop"
  ) %>%
  arrange(<<dimension_1>>, <<dimension_2>>)

# --- OPTION B : SQL pur ---
# df_gold <- DBI::dbGetQuery(sc, "
#   SELECT
#     <<dimension_1>>,
#     <<dimension_2>>,
#     COUNT(*)                              AS nb,
#     SUM(<<montant>>)                      AS total,
#     AVG(<<montant>>)                      AS moyenne,
#     PERCENTILE_APPROX(<<montant>>, 0.5)   AS mediane
#   FROM nessie.silver.<<nom_de_la_table>>
#   GROUP BY <<dimension_1>>, <<dimension_2>>
#   ORDER BY <<dimension_1>>, <<dimension_2>>
# ") %>% copy_to(sc, ., overwrite = TRUE)

# ============================================================

cat(sprintf("Gold : %s lignes × %d colonnes\n",
            format(sdf_nrow(df_gold), big.mark = ","), ncol(df_gold)))

# --- ÉCRITURE GOLD ICEBERG ---
TABLE_GOLD <- "nessie.gold.<<nom_du_resultat>>"
DBI::dbExecute(sc, "CREATE NAMESPACE IF NOT EXISTS nessie.gold")
spark_write_table(df_gold, TABLE_GOLD, mode = "overwrite", format = "iceberg")
cat(sprintf("✓ Table Gold écrite : %s\n", TABLE_GOLD))

# --- EXPORT CSV VERS STAGING (pour partage externe) ---
# df_gold_r <- collect(df_gold)   # Rapatrier en R (résultat agrégé = petit)
# s3 <- arrow::S3FileSystem$create(
#   access_key        = MINIO_ACCESS_KEY,
#   secret_key        = MINIO_SECRET_KEY,
#   endpoint_override = sub("^https?://", "", MINIO_ENDPOINT),
#   scheme            = "http", region = "us-east-1"
# )
# arrow::write_csv_arrow(df_gold_r,
#   s3$path("staging/exports/<<nom_du_resultat>>.csv"))
# cat("✓ CSV exporté vers staging/exports/\n")

spark_disconnect(sc)
