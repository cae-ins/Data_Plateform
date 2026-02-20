#FORMAT DU SCRIPT A METTRE SUR MINIO


from pyspark.sql import SparkSession
import os

# On récupère les endpoints via des variables d'env pour plus de flexibilité
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.mon-namespace.svc.cluster.local:80")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie.trino.svc.cluster.local:19120/api/v1")

# On crée la session sans redéfinir toute la conf (elle sera injectée par le YAML)
spark = SparkSession.builder.getOrCreate()

path_minio = "<<CHEMIN VERS FICHIER SUR MINIO>>"

# Lecture Excel
df_fusionne = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load(path_minio)

table_target = "nessie.bronze.<<nom de la table>>"

# Logique Iceberg
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
df_fusionne.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(table_target)

spark.stop()