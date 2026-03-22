# ============================================================
# TRANSFORMATION BRONZE → SILVER (R / sparklyr)
# ============================================================
# Équivalent R de bronze-silver.ipynb
# Lit une table Bronze Iceberg, applique les transformations
# métier (cast, nettoyage, règles) et écrit en Silver.
# ============================================================

library(sparklyr)
library(dplyr)

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
# LECTURE DE LA TABLE BRONZE
# ============================================================

TABLE_BRONZE <- "nessie.bronze.<<nom_de_la_table>>"

df <- spark_read_table(sc, TABLE_BRONZE)
cat(sprintf("Bronze : %s lignes × %d colonnes\n",
            format(sdf_nrow(df), big.mark = ","), ncol(df)))

# ============================================================
# ON EFFECTUE LES TRANSFORMATIONS SILVER ICI
# ============================================================
# Silver = données propres, typées, déduplicées, règles métier.
# On peut travailler en dplyr (ci-dessous) ou en SQL pur.

# --- OPTION A : dplyr (syntaxe R naturelle) ---
df_silver <- df %>%

  # 1. Cast des types (Bronze = tout String)
  # mutate(montant_brut = as.numeric(montant_brut)) %>%
  # mutate(periode      = as.Date(periode, "%Y-%m")) %>%

  # 2. Nettoyage
  # mutate(matricule = trimws(toupper(matricule))) %>%

  # 3. Filtrage
  # filter(!is.na(matricule)) %>%
  # filter(montant_brut > 0) %>%

  # 4. Colonnes calculées
  # mutate(annee = year(periode), mois = month(periode)) %>%

  # 5. Déduplication
  # distinct(matricule, periode, .keep_all = TRUE) %>%

  identity()  # ← retirer cette ligne quand les transformations sont renseignées


# --- OPTION B : SQL pur via DBI ---
# tbl(sc, TABLE_BRONZE) %>% ... est aussi possible
# ou directement :
# df_silver <- DBI::dbGetQuery(sc, "
#   SELECT
#     TRIM(UPPER(matricule))           AS matricule,
#     CAST(montant_brut AS DOUBLE)     AS montant_brut,
#     TO_DATE(periode, 'yyyy-MM')      AS periode,
#     YEAR(TO_DATE(periode, 'yyyy-MM')) AS annee
#   FROM nessie.bronze.<<nom_de_la_table>>
#   WHERE matricule IS NOT NULL
# ") %>% copy_to(sc, ., overwrite = TRUE)

# ============================================================

cat(sprintf("Silver : %s lignes × %d colonnes\n",
            format(sdf_nrow(df_silver), big.mark = ","), ncol(df_silver)))

TABLE_SILVER <- "nessie.silver.<<nom_de_la_table>>"

DBI::dbExecute(sc, "CREATE NAMESPACE IF NOT EXISTS nessie.silver")

spark_write_table(df_silver, TABLE_SILVER, mode = "overwrite", format = "iceberg")

cat(sprintf("✓ Table écrite : %s\n", TABLE_SILVER))

spark_disconnect(sc)
