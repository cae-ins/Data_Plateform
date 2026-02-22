# Templates — Accès MinIO & Data Platform

Bibliothèque de templates pour importer et exporter des données depuis/vers MinIO,
selon votre environnement et votre cas d'usage.

---

## Démarrage rapide

```bash
# 1. Copier le fichier de credentials
cp templates/.env.example .env

# 2. Remplir vos valeurs dans .env (endpoint, clés)

# 3. Choisir le template qui correspond à votre cas
```

> **Ne jamais commiter `.env`** — vérifier que `.gitignore` contient bien `.env`

---

## Quelle technologie choisir ?

| Situation | Template recommandé |
|-----------|---------------------|
| Analyse exploratoire, machine locale, petit fichier | `python/` ou `R/` |
| Pipeline R (panel_admin, ENE, IPM) | `R/` |
| Pipeline Python (nowcasting, GeoAI, CNPS) | `python/` |
| Ingestion en production vers Bronze/Silver | `spark/` |
| Planification automatique (Airflow) | `spark/yaml/` + `dag_airflow.py` |

---

## Templates Python (standalone)

> Dépendances : `pip install minio python-dotenv pandas pyarrow s3fs openpyxl pyreadstat`

| Fichier | Description |
|---------|-------------|
| `python/01_read_excel_from_minio.py` | Lire un `.xlsx` depuis MinIO → DataFrame pandas |
| `python/02_read_parquet_from_minio.py` | Lire un `.parquet` depuis MinIO → pandas ou Polars |
| `python/03_read_dta_from_minio.py` | Lire un `.dta` Stata depuis MinIO (préserve les labels) |
| `python/04_read_csv_from_minio.py` | Lire un `.csv` depuis MinIO → DataFrame pandas |
| `python/05_write_parquet_to_minio.py` | Écrire un DataFrame → Parquet sur MinIO |
| `python/06_write_csv_to_minio.py` | Écrire un DataFrame → CSV sur MinIO (compatible Excel FR) |

---

## Templates R

> Dépendances : `install.packages(c("arrow", "aws.s3", "dotenv", "data.table", "haven", "readxl"))`

| Fichier | Description |
|---------|-------------|
| `R/01_read_from_minio.R` | Lire Parquet, CSV, DTA, Excel depuis MinIO |
| `R/02_write_to_minio.R` | Écrire Parquet, CSV, DTA vers MinIO |

**Note** : La lecture/écriture Parquet via `arrow` est directe (streaming).
Pour les autres formats, le template télécharge dans un fichier temporaire puis lit localement.

---

## Templates Spark (production K8s)

> Pour la mise en production via Airflow + Spark Operator.
> Voir aussi : `dag_airflow.py` à la racine du dépôt.

### Scripts Python (à uploader sur `s3a://sparkapplication/`)

| Fichier | Description |
|---------|-------------|
| `spark/01_excel_to_bronze.py` | Excel staging → table Iceberg Bronze |
| `spark/02_parquet_to_bronze.py` | Parquet staging → table Iceberg Bronze |
| `spark/03_dta_to_bronze.py` | Stata `.dta` staging → table Iceberg Bronze |
| `spark/04_bronze_to_silver.py` | Table Bronze → Table Silver (transformations SQL) |

### YAML Spark Operator (à appliquer sur le cluster)

| Fichier | Description |
|---------|-------------|
| `spark/yaml/00_secret_credentials.yaml` | **À appliquer une fois** : Secret K8s pour les credentials MinIO |
| `spark/yaml/01_excel_to_bronze.yaml` | Job Spark pour Excel → Bronze (inclut spark-excel) |
| `spark/yaml/02_parquet_or_dta_to_bronze.yaml` | Job Spark pour Parquet/DTA → Bronze |
| `spark/yaml/03_bronze_to_silver.yaml` | Job Spark pour Bronze → Silver |

### Déploiement d'un nouveau job

```bash
# Étape 1 : Appliquer le secret (une seule fois par namespace)
kubectl apply -f templates/spark/yaml/00_secret_credentials.yaml -n airflow

# Étape 2 : Uploader le script Python sur MinIO
mc cp templates/spark/01_excel_to_bronze.py minio/sparkapplication/

# Étape 3 : Adapter le YAML (nom du job, table cible)
# Modifier : metadata.name et les variables dans le script

# Étape 4 : Appliquer le job
kubectl apply -f templates/spark/yaml/01_excel_to_bronze.yaml -n airflow

# Étape 5 : Surveiller
kubectl get sparkapplication -n airflow
kubectl logs <pod-driver> -n airflow
```

---

## Endpoints selon l'environnement

| Environnement | MINIO_ENDPOINT | NESSIE_URI |
|---------------|----------------|------------|
| Machine locale | `http://192.168.1.230:30137` | `http://192.168.1.230:30604/api/v1` |
| JupyterHub (cluster) | `http://minio.mon-namespace.svc.cluster.local:80` | `http://nessie.trino.svc.cluster.local:19120/api/v1` |

---

## Architecture des buckets MinIO

```
staging/    ← Données brutes déposées par les équipes (Excel, CSV, DTA, Parquet)
bronze/     ← Tables Iceberg ingérées par Spark (données brutes typées)
silver/     ← Tables Iceberg transformées (données propres, règles métier)
gold/       ← Tables agrégées pour les dashboards et exports finaux
sparkapplication/ ← Scripts Python Spark (.py) référencés par les YAML
```

---

## Sécurité

- Les credentials ne doivent **jamais** être écrits en dur dans les scripts ou YAML de job.
- En **développement** : utiliser le fichier `.env` (non commité).
- En **production** (K8s) : utiliser le Secret `minio-credentials` via `secretKeyRef`.
- Le fichier `spark/yaml/00_secret_credentials.yaml` contient des valeurs encodées en base64 —
  à régénérer si les credentials changent :
  ```bash
  echo -n "mon-nouveau-password" | base64
  ```
