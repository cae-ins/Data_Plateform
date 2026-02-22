# ============================================================
# EXPLORATION — REQUÊTAGE DES TABLES ICEBERG (R / sparklyr)
# ============================================================
# Équivalent R de exploration.ipynb
# Interroge les tables Bronze/Silver depuis R avec sparklyr
# ou via Trino (DBI + RJDBC) pour un accès SQL direct.
# ============================================================

library(sparklyr)
library(dplyr)
library(data.table)

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


# ============================================================
# OPTION A : sparklyr (accès natif Iceberg + Nessie)
# ============================================================

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

# Lister les tables disponibles
DBI::dbGetQuery(sc, "SHOW NAMESPACES IN nessie")
DBI::dbGetQuery(sc, "SHOW TABLES IN nessie.bronze")
DBI::dbGetQuery(sc, "SHOW TABLES IN nessie.silver")

# Lecture d'une table → pointeur Spark (lazy, pas encore en mémoire)
df <- spark_read_table(sc, "nessie.silver.<<nom_de_la_table>>")

# ============================================================
# ON EFFECTUE LES REQUÊTES ET ANALYSES ICI
# ============================================================

# --- Agrégation via dplyr (lazy) ---
resultat <- df %>%
  group_by(<<colonne_groupe>>) %>%
  summarise(
    nb      = n(),
    total   = sum(<<montant>>, na.rm = TRUE),
    moyenne = mean(<<montant>>, na.rm = TRUE)
  ) %>%
  arrange(desc(total))

# Rapatrier en R (collect) uniquement les résultats agrégés
df_local <- collect(resultat)
print(df_local)

# --- Requête SQL directe ---
# df_local <- DBI::dbGetQuery(sc, "
#   SELECT
#     <<colonne_groupe>>,
#     COUNT(*) AS nb,
#     SUM(<<montant>>) AS total
#   FROM nessie.silver.<<nom_de_la_table>>
#   GROUP BY <<colonne_groupe>>
#   ORDER BY total DESC
# ")

# --- Time travel Iceberg (via SQL) ---
# DBI::dbGetQuery(sc,
#   "SELECT * FROM nessie.silver.ma_table
#    TIMESTAMP AS OF '2024-01-01 00:00:00'
#    LIMIT 10")

# --- Historique des snapshots ---
# DBI::dbGetQuery(sc, "SELECT * FROM nessie.silver.ma_table.snapshots")

# ============================================================

spark_disconnect(sc)


# ============================================================
# OPTION B : Trino via DBI (plus léger, SQL pur, pas de Spark)
# ============================================================
# Recommandé quand on veut juste lire des données sans transformer.
# Nécessite : install.packages(c("DBI", "RJDBC"))
#
# library(DBI)
# library(RJDBC)
#
# TRINO_HOST <- "192.168.1.230"   # ou nom DNS sur le cluster
# TRINO_PORT <- 30669             # port NodePort Trino
#
# drv  <- JDBC("io.trino.jdbc.TrinoDriver",
#              "<<chemin>>/trino-jdbc-xxx.jar")
# conn <- dbConnect(drv,
#   url      = sprintf("jdbc:trino://%s:%d/iceberg/silver", TRINO_HOST, TRINO_PORT),
#   user     = "trino",
#   password = "")
#
# df_local <- dbGetQuery(conn, "
#   SELECT *
#   FROM silver.<<nom_de_la_table>>
#   LIMIT 1000
# ")
# dbDisconnect(conn)
