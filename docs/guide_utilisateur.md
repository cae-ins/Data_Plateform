# Guide Utilisateur — Data Platform ANSTAT

## Table des matières

1. [Pourquoi cette plateforme ?](#1-pourquoi-cette-plateforme-)
2. [Architecture et concepts clés](#2-architecture-et-concepts-clés)
3. [Accès et prérequis](#3-accès-et-prérequis)
4. [Flux de travail standard](#4-flux-de-travail-standard)
5. [Étape 1 — Déposer ses données sur MinIO](#5-étape-1--déposer-ses-données-sur-minio)
6. [Étape 2 — Travailler sur les données](#6-étape-2--travailler-sur-les-données)
   - [En Python](#en-python--jupyter-notebook)
   - [En R](#en-r--script-r)
   - [En Stata](#en-stata--do-file)
7. [Les couches de données : Bronze, Silver, Gold](#7-les-couches-de-données--bronze-silver-gold)
8. [Étape 3 — Passer en production](#8-étape-3--passer-en-production)
9. [Bonnes pratiques](#9-bonnes-pratiques)
10. [FAQ](#10-faq)

---

## 1. Pourquoi cette plateforme ?

Avant la Data Platform, chaque projet gérait ses données en silo : fichiers Excel sur des disques locaux, scripts R ou Stata dispersés, pas de traçabilité des transformations, pas de point d'accès commun.

La Data Platform résout ces problèmes en offrant :

- **Un entrepôt unique** : toutes les données sont stockées dans MinIO, accessible depuis n'importe quel poste ou depuis JupyterHub.
- **Un catalogue versionné** : chaque table est une table Iceberg gérée par Nessie. On peut revenir à n'importe quelle version passée d'une table, comme avec `git` pour le code.
- **Un espace de travail commun** : les données traitées par un agent R sont immédiatement consultables par un autre agent Python ou Stata, sans copier de fichier.
- **Une mise en production standardisée** : un script validé en dev se déploie en production via un simple fichier YAML, sans réécrire la logique.
- **La compatibilité avec tous les outils ANSTAT** : Python, R et Stata peuvent tous lire et déposer des données sur la plateforme.

---

## 2. Architecture et concepts clés

### Schéma d'ensemble

```
┌─────────────────────────────────────────────────────────────────────┐
│                         VOS SOURCES DE DONNÉES                      │
│   Fichiers Excel · CSV · Stata .dta · Parquet · Résultats de modèles│
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dépôt manuel ou upload depuis script
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        MinIO — STAGING                              │
│  Stockage objet S3. Zone de dépôt intermédiaire.                    │
│  Accessible depuis votre machine, JupyterHub, R, Python, Stata.     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  job Spark (notebook ou script K8s)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   MinIO BRONZE — Tables Iceberg                     │
│  Données brutes ingérées, tout en String, aucune perte.             │
│  Versionnées par Nessie (historique complet des modifications).      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  transformation (cast, nettoyage, règles)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   MinIO SILVER — Tables Iceberg                     │
│  Données propres, typées, déduplicées, règles métier appliquées.    │
│  Source de vérité pour les analyses.                                 │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  agrégation
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    MinIO GOLD — Tables Iceberg                      │
│  Résultats agrégés, prêts pour dashboards et exports finaux.        │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
                     Trino · Exports CSV · Dashboards
```

### Les composants expliqués simplement

| Composant | Rôle | Analogie |
|-----------|------|----------|
| **MinIO** | Stockage des fichiers et des tables. Accessible comme un serveur de fichiers S3. | Dropbox d'entreprise |
| **Apache Iceberg** | Format de table qui rend les données ACID (transactions, rollback) et interrogeables en SQL. | Remplace les fichiers Parquet "à plat" par de vraies tables |
| **Nessie** | Catalogue qui gère l'historique de toutes les tables Iceberg. Chaque modification est enregistrée. | Git, mais pour les données |
| **Apache Spark** | Moteur de calcul distribué qui lit, transforme et écrit les tables Iceberg. | Le "processeur" de la plateforme |
| **Airflow** | Ordonnanceur qui déclenche les jobs Spark automatiquement (quotidien, mensuel, etc.). | Crontab évolué avec interface graphique |
| **Trino** | Moteur SQL qui permet d'interroger les tables Iceberg sans lancer Spark. Plus léger, idéal pour l'exploration. | Requêtes SQL directes sur les tables |
| **Spark Operator** | Plugin Kubernetes qui exécute les jobs Spark en production sur le cluster. | Gestionnaire d'exécution cloud |

### Les buckets MinIO

```
staging/          ← Vous déposez vos fichiers ici (Excel, CSV, DTA, Parquet)
bronze/           ← Tables Iceberg ingérées par Spark (automatique)
silver/           ← Tables Iceberg transformées (automatique)
gold/             ← Tables Iceberg agrégées (automatique)
sparkapplication/ ← Scripts Python .py pour les jobs Spark production
```

> **Règle simple** : vous ne déposez que dans `staging/`. Les autres buckets sont écrits par Spark.

---

## 3. Accès et prérequis

### 3.1 Endpoints selon votre environnement

Deux situations possibles : vous travaillez **depuis votre machine locale** ou **depuis JupyterHub** (dans le cluster).

| | Depuis votre machine | Depuis JupyterHub |
|--|----------------------|-------------------|
| **MinIO** | `http://192.168.1.230:30137` | `http://minio.mon-namespace.svc.cluster.local:80` |
| **Nessie** | `http://192.168.1.230:30604/api/v1` | `http://nessie.trino.svc.cluster.local:19120/api/v1` |
| **Console MinIO** (navigateur) | `http://192.168.1.230:30137` | *(accès réseau local)* |

Tous les fichiers de travail contiennent les deux blocs commentés — il suffit d'activer le bon.

### 3.2 Identifiants

```
Utilisateur MinIO : datalab-team
Mot de passe MinIO : minio-datalabteam123
```

Ces identifiants sont à mettre dans un fichier `.env` à la racine de votre projet :

```bash
# Copier le template
cp templates/.env.example .env
# Puis ouvrir .env et vérifier les valeurs
```

**Ne jamais écrire les identifiants en dur dans un script commité sur Git.**

### 3.3 Packages à installer

#### Python

```bash
pip install minio python-dotenv pandas pyarrow s3fs openpyxl pyreadstat pyspark sparklyr
```

#### R

```r
install.packages(c(
  "sparklyr",    # Interface Spark pour R
  "dplyr",       # Manipulation de données (syntaxe naturelle)
  "arrow",       # Lecture/écriture Parquet, accès S3
  "aws.s3",      # Téléchargement/upload vers MinIO
  "dotenv",      # Chargement du fichier .env
  "data.table",  # Lecture CSV rapide
  "haven",       # Lecture/écriture Stata .dta
  "readxl",      # Lecture Excel
  "DBI"          # Interface SQL (Trino)
))
```

#### Stata

Stata 16 ou supérieur requis pour les blocs `python:`.

```stata
* Vérifier la version Python connectée à Stata
python query
* Installer les packages Python nécessaires (dans le terminal système) :
*   pip install minio pyarrow pandas pyreadstat
```

---

## 4. Flux de travail standard

Quel que soit votre langage, le flux est toujours le même :

```
1. DÉPOSER    Vos données brutes → MinIO staging
      │
      ▼
2. TRAITER    Ouvrir le fichier de travail correspondant
              (notebook / script R / do-file)
              → Charger les données
              → Appliquer vos traitements
              → Écrire le résultat (Bronze ou Silver)
      │
      ▼
3. PRODUIRE   (optionnel) Convertir le script en job Spark production
              → YAML Spark Operator + DAG Airflow
```

Chaque fichier de travail à la racine correspond à une étape et un langage :

```
local-bronze.{ipynb|R|do}     ← données sur votre machine → Bronze
staging-bronze.{ipynb|R|do}   ← données sur MinIO staging → Bronze
bronze-silver.{ipynb|R}       ← Bronze → Silver (transformations)
exploration.{ipynb|R|do}      ← requêtes et analyses sur Silver/Gold
silver-gold.{ipynb|R}         ← Silver → Gold (agrégations)
```

---

## 5. Étape 1 — Déposer ses données sur MinIO

Avant de travailler sur la plateforme, vos données brutes doivent être dans le bucket `staging`.

### Via la console web (recommandé pour les petits fichiers)

1. Ouvrir `http://192.168.1.230:30137` dans votre navigateur
2. Se connecter : `datalab-team` / `minio-datalabteam123`
3. Cliquer sur le bucket `staging`
4. Créer un dossier portant le nom de votre projet (ex. `cnps/`, `ene/`, `panel_admin/`)
5. Glisser-déposer vos fichiers

### Via Python (pour les gros fichiers ou l'automatisation)

Utiliser le template `templates/python/05_write_parquet_to_minio.py` ou `06_write_csv_to_minio.py`.

Exemple minimal pour uploader n'importe quel fichier :

```python
from minio import Minio

client = Minio(
    "192.168.1.230:30137",
    access_key="datalab-team",
    secret_key="minio-datalabteam123",
    secure=False
)
client.fput_object("staging", "mon_projet/mon_fichier.xlsx", "/chemin/local/mon_fichier.xlsx")
```

### Via R

```r
library(aws.s3)
Sys.setenv(
  AWS_ACCESS_KEY_ID     = "datalab-team",
  AWS_SECRET_ACCESS_KEY = "minio-datalabteam123",
  AWS_DEFAULT_REGION    = "us-east-1"
)
aws.s3::put_object(
  file     = "/chemin/local/mon_fichier.csv",
  object   = "mon_projet/mon_fichier.csv",
  bucket   = "staging",
  base_url = "192.168.1.230:30137",
  region   = ""
)
```

### Formats acceptés

| Format | Extension | Recommandé pour |
|--------|-----------|-----------------|
| Parquet | `.parquet` | Gros volumes, sorties R/Python |
| Excel | `.xlsx` | Données brutes reçues |
| CSV | `.csv` | Exports simples, compatibilité universelle |
| Stata | `.dta` | Données issues de pipelines Stata/CNPS/ENE |

---

## 6. Étape 2 — Travailler sur les données

### En Python — Jupyter Notebook

Ouvrir le notebook correspondant à votre étape sur JupyterHub ou en local.

#### Structure commune à tous les notebooks

Chaque notebook suit la même organisation en 4 parties :

**Partie 1 — Configuration** : deux blocs d'endpoints (local / JHub), activer le bon.

```python
# DEPUIS SA MACHINE LOCALE
MINIO_ENDPOINT = "http://192.168.1.230:30137"
NESSIE_URI     = "http://192.168.1.230:30604/api/v1"

# DEPUIS JHUB (décommenter)
# MINIO_ENDPOINT = "http://minio.mon-namespace.svc.cluster.local:80"
# NESSIE_URI     = "http://nessie.trino.svc.cluster.local:19120/api/v1"
```

**Partie 2 — SparkConf** : configuration complète injectée (packages, catalogue Nessie, accès MinIO). **Ne pas modifier**, sauf la mémoire driver si nécessaire.

**Partie 3 — Chargement des données** : lire depuis un fichier local, MinIO staging, ou une table Bronze/Silver.

**Partie 4 — Traitements** : cellule vide balisée `# ON EFFECTUE LES TRAITEMENTS ICI` — c'est ici que vous écrivez votre logique.

**Partie 5 — Écriture** : écriture dans la table Iceberg cible.

#### Quel notebook ouvrir ?

| Situation | Notebook |
|-----------|----------|
| J'ai un fichier sur mon ordinateur à ingérer | `local-bronze.ipynb` |
| J'ai déposé un fichier sur MinIO staging | `staging-bronze.ipynb` |
| Je veux nettoyer/transformer une table Bronze | `bronze-silver.ipynb` |
| Je veux explorer ou requêter des tables | `exploration.ipynb` |
| Je veux produire des agrégations finales | `silver-gold.ipynb` |

#### Les variables à toujours modifier

Dans chaque notebook, repérer les sections balisées et les adapter :

```python
# Dans staging-bronze.ipynb
path_minio = "s3a://staging/mon_projet/mon_fichier.xlsx"  # ← votre chemin

# Dans bronze-silver.ipynb
TABLE_BRONZE = "nessie.bronze.ma_table"   # ← nom de votre table source
TABLE_SILVER = "nessie.silver.ma_table"   # ← nom de votre table cible

# Dans silver-gold.ipynb
TABLE_GOLD = "nessie.gold.mon_resultat"   # ← nom du résultat
```

#### Convention de nommage des tables

```
nessie.<couche>.<projet>_<description>

Exemples :
  nessie.bronze.panel_admin_solde_mensuel
  nessie.silver.cnps_declarations_nettoyees
  nessie.gold.ene_taux_chomage_regional
```

---

### En R — Script R

Ouvrir le script `.R` correspondant dans RStudio ou l'exécuter depuis JupyterHub.

#### Structure commune à tous les scripts R

La structure est identique aux notebooks Python, traduite en R :

**Configuration** : choisir l'endpoint (local ou JHub).

```r
# DEPUIS SA MACHINE LOCALE
MINIO_ENDPOINT <- "http://192.168.1.230:30137"
NESSIE_URI     <- "http://192.168.1.230:30604/api/v1"

# DEPUIS JHUB (décommenter)
# MINIO_ENDPOINT <- "http://minio.mon-namespace.svc.cluster.local:80"
# NESSIE_URI     <- "http://nessie.trino.svc.cluster.local:19120/api/v1"
```

**spark_config()** : configuration complète de la session Spark. Équivalent du `SparkConf()` Python.

**Connexion Spark** :

```r
sc <- spark_connect(master = "local", config = config)
```

**Chargement** : lecture d'un fichier ou d'une table Iceberg.

**Traitements** : section balisée `# ON EFFECTUE LES TRAITEMENTS ICI`.

**Écriture** :

```r
spark_write_table(df_silver, "nessie.silver.ma_table",
                  mode = "overwrite", format = "iceberg")
```

**Déconnexion** :

```r
spark_disconnect(sc)
```

#### Transformer les données avec dplyr (syntaxe R naturelle)

Dans `bronze-silver.R`, les transformations s'écrivent en `dplyr`, qui sera automatiquement traduit en Spark SQL :

```r
df_silver <- df %>%
  mutate(montant_brut = as.numeric(montant_brut)) %>%
  mutate(matricule    = trimws(toupper(matricule))) %>%
  filter(!is.na(matricule), montant_brut > 0) %>%
  mutate(annee = year(as.Date(periode, "%Y-%m"))) %>%
  distinct(matricule, periode, .keep_all = TRUE)
```

#### Écrire directement du SQL dans R

```r
resultat <- DBI::dbGetQuery(sc, "
  SELECT annee, COUNT(*) AS nb, SUM(montant_brut) AS total
  FROM nessie.silver.ma_table
  GROUP BY annee
  ORDER BY annee
")
```

#### Templates légers (sans Spark)

Pour les opérations simples qui ne nécessitent pas Spark (lire/écrire des fichiers sur MinIO sans passer par Iceberg), utiliser les templates dans `templates/R/` :

- `templates/R/01_read_from_minio.R` — lire Parquet, CSV, DTA, Excel depuis staging
- `templates/R/02_write_to_minio.R` — écrire Parquet, CSV, DTA vers staging

---

### En Stata — Do-file

#### Principe : Stata traite, Python transfère

Stata ne peut pas se connecter directement à Iceberg ou à Nessie. Le flux pour un utilisateur Stata est donc :

```
MinIO staging
     │  (téléchargement via bloc python:)
     ▼
Stata (traitement do-file)
     │  (upload via bloc python:)
     ▼
MinIO staging
     │  (job Spark déclenché manuellement ou via Airflow)
     ▼
Bronze / Silver Iceberg
```

Les do-files utilisent la fonctionnalité **Python intégré dans Stata** (disponible depuis Stata 16) pour les parties MinIO, et font tout le traitement statistique nativement en Stata.

#### Structure commune à tous les do-files

**Configuration** :

```stata
global MINIO_ENDPOINT   "http://192.168.1.230:30137"
global MINIO_ACCESS_KEY "datalab-team"
global MINIO_SECRET_KEY "minio-datalabteam123"
```

**Téléchargement depuis MinIO** (bloc `python:`):

```stata
python:
from minio import Minio
client = Minio("192.168.1.230:30137",
               access_key="datalab-team",
               secret_key="minio-datalabteam123",
               secure=False)
client.fget_object("staging", "mon_projet/mon_fichier.dta", "/tmp/fichier.dta")
end

use "/tmp/fichier.dta", clear
```

**Traitements Stata** (section `ON EFFECTUE LES TRAITEMENTS ICI`) :

```stata
* Vos commandes Stata habituelles
keep if annee >= 2020
recode sexe (1=0 "Homme") (2=1 "Femme"), gen(femme)
regress salaire femme age experience
```

**Upload du résultat** (bloc `python:`) :

```stata
export delimited "/tmp/resultat.csv", delimiter(";") replace

python:
import pandas as pd, pyarrow as pa, pyarrow.parquet as pq, io
from minio import Minio
df = pd.read_csv("/tmp/resultat.csv", sep=";")
buf = io.BytesIO()
pq.write_table(pa.Table.from_pandas(df), buf, compression="snappy")
buf.seek(0)
Minio("192.168.1.230:30137", access_key="datalab-team",
      secret_key="minio-datalabteam123", secure=False
).put_object("staging", "mon_projet/resultat.parquet", buf,
             length=buf.getbuffer().nbytes)
end
```

#### Quel do-file ouvrir ?

| Situation | Do-file |
|-----------|---------|
| J'ai un fichier local à ingérer | `local-bronze.do` |
| J'ai un fichier sur MinIO à traiter | `staging-bronze.do` |
| Je veux consulter un export depuis la plateforme | `exploration.do` |

---

## 7. Les couches de données : Bronze, Silver, Gold

### Bronze — Ingestion brute

**Règle** : le Bronze ne transforme rien. On ingère les données telles quelles, avec le minimum nécessaire pour les stocker (renommage de colonnes si espaces, pas de cast de types).

- Tout est stocké en `String` pour ne rien perdre
- La table Bronze est la source de vérité sur les données brutes
- En cas d'erreur dans la transformation Silver, on peut toujours reprendre depuis Bronze

```python
# Bronze : on ne caste PAS les types
df.write.format("iceberg").mode("overwrite").saveAsTable("nessie.bronze.ma_table")
```

### Silver — Données propres

**Règle** : Silver = Bronze + types corrects + règles métier + déduplication.

Transformations typiques appliquées en Silver :

```python
df_silver = (
    df_bronze
    .withColumn("montant_brut", col("montant_brut").cast("double"))
    .withColumn("periode",      to_date("periode", "yyyy-MM"))
    .withColumn("matricule",    trim(upper(col("matricule"))))
    .filter(col("matricule").isNotNull())
    .dropDuplicates(["matricule", "periode"])
)
```

### Gold — Résultats agrégés

**Règle** : Gold = Silver agrégé pour un usage précis (dashboard, rapport, indicateur).

```python
df_gold = spark.sql("""
    SELECT annee, region, COUNT(*) AS nb_agents, AVG(salaire) AS salaire_moyen
    FROM nessie.silver.panel_admin_solde
    GROUP BY annee, region
""")
df_gold.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.salaires_regionaux")
```

### Time travel — revenir à une version passée

Iceberg garde l'historique complet de chaque table. Pour lire une version passée :

```python
# Python / Spark
spark.read.option("as-of-timestamp", "2024-06-01 00:00:00") \
     .table("nessie.silver.ma_table")

# SQL
spark.sql("SELECT * FROM nessie.silver.ma_table TIMESTAMP AS OF '2024-06-01'")
```

```r
# R / sparklyr
DBI::dbGetQuery(sc,
  "SELECT * FROM nessie.silver.ma_table TIMESTAMP AS OF '2024-06-01'")
```

---

## 8. Étape 3 — Passer en production

Le passage en production consiste à transformer votre script de développement en un **job Spark automatisé** qui s'exécute sur le cluster Kubernetes.

### Principe de séparation code / configuration

En développement, le notebook contient toute la configuration Spark (mémoire, packages, endpoints). En production, **cette configuration est retirée du script** et portée par un fichier YAML — le script devient minimal.

| Dev (notebook / script) | Prod (script K8s) |
|-------------------------|-------------------|
| `SparkConf()` avec mémoire, packages, endpoints | `SparkSession.builder.getOrCreate()` seulement |
| Exécuté sur votre machine | Exécuté sur le cluster |
| Config codée en dur | Config injectée par le YAML |

### Étapes concrètes

**1. Partir du notebook validé** (ex. `staging-bronze.ipynb`)

**2. Créer le script de production** en copiant la logique dans un fichier `.py` minimal :

```python
# mon_job.py  ← ce fichier sera uploadé sur MinIO
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()  # ← toute la config vient du YAML

df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("s3a://staging/mon_projet/mon_fichier.xlsx")

# ... vos transformations ...

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
df.write.format("iceberg").mode("overwrite").saveAsTable("nessie.bronze.ma_table")
spark.stop()
```

**3. Uploader le script sur MinIO** :

```bash
mc cp mon_job.py minio/sparkapplication/mon_job.py
```

**4. Adapter le YAML** depuis `templates/spark/yaml/` :

Modifier au minimum :
- `metadata.name` : nom unique du job
- `mainApplicationFile` : `s3a://sparkapplication/mon_job.py`
- Les packages : retirer `spark-excel` si le fichier n'est pas Excel

**5. Appliquer le Secret** (une seule fois par namespace) :

```bash
kubectl apply -f templates/spark/yaml/00_secret_credentials.yaml -n airflow
```

**6. Lancer le job** :

```bash
kubectl apply -f mon_yaml.yaml -n airflow
# Surveiller
kubectl get sparkapplication -n airflow
kubectl logs <nom-du-pod-driver> -n airflow
```

**7. (Optionnel) Planifier via Airflow**

Ouvrir `dag_airflow.py`, remplacer le contenu YAML par votre YAML adapté, déployer le DAG dans Airflow.

### Packages disponibles selon le type de fichier source

| Source | Package supplémentaire à ajouter dans le YAML |
|--------|-----------------------------------------------|
| Excel `.xlsx` | `com.crealytics:spark-excel_2.12:3.5.0_0.20.3` |
| Parquet | *(aucun, natif Spark)* |
| CSV | *(aucun, natif Spark)* |
| Stata `.dta` | *(pont pandas via pyreadstat — voir `templates/spark/03_dta_to_bronze.py`)* |

---

## 9. Bonnes pratiques

### Nommage des tables Iceberg

```
nessie.<couche>.<projet>_<description>_<granularité si utile>

# Bien
nessie.bronze.panel_admin_solde_mensuel
nessie.silver.cnps_declarations_nettoyees
nessie.gold.ene_taux_emploi_trimestriel

# À éviter
nessie.bronze.test
nessie.silver.data2
nessie.gold.resultat_final_v3
```

### Organisation dans MinIO staging

```
staging/
  <projet>/
    <sous-dossier ou periode>/
      fichier.xlsx

# Exemples
staging/panel_admin/2024_01/solde.xlsx
staging/cnps/declarations/2024_T1.dta
staging/ene/poids/poids_calibres_2024.parquet
```

### Formats recommandés

| Usage | Format recommandé | Raison |
|-------|-------------------|--------|
| Stocker des résultats R/Python | Parquet (snappy) | Rapide, compact, lisible partout |
| Partager avec Excel | CSV (`;`, UTF-8 BOM) | Compatible Excel français |
| Partager avec Stata | `.dta` version 15 | Préserve les labels |
| Ingestion Bronze → Silver | Iceberg | Transactions ACID, time travel |

### Gestion des credentials

- En **développement** : fichier `.env` à la racine de votre projet (jamais commité sur Git)
- En **production** (cluster K8s) : Kubernetes Secret `minio-credentials` — les identifiants ne figurent pas dans les YAML
- Vérifier que `.gitignore` contient `.env`

### Mémoire Spark — valeurs de référence

| Taille du fichier / table | Driver | Executor |
|--------------------------|--------|----------|
| < 500 Mo | 4g | 4g |
| 500 Mo – 5 Go | 8g | 4g |
| 5 Go – 30 Go (ex. panel_admin 31M lignes) | 16g | 8g |
| > 30 Go | 16g | 16g, instances = 4 |

### Ne pas faire

- Ne pas écrire les identifiants en dur dans un script versionné
- Ne pas faire `.toPandas()` ou `collect()` sur une table complète non filtrée
- Ne pas modifier directement les tables Bronze — toujours passer par un script versionné
- Ne pas utiliser de noms de table avec espaces ou accents

---

## 10. FAQ

**Q : Je ne sais pas si je dois utiliser `local-bronze` ou `staging-bronze`.**

R : Si votre fichier est encore sur votre ordinateur, commencez par `local-bronze` (ou déposez d'abord sur staging via la console MinIO, puis utilisez `staging-bronze`). La différence : `local-bronze` lit depuis votre disque, `staging-bronze` lit depuis MinIO.

---

**Q : J'obtiens une erreur de connexion à MinIO.**

R : Vérifier deux points :
1. L'endpoint correspond à votre environnement (local vs JHub)
2. L'endpoint ne contient pas `http://` quand il est passé au client MinIO Python — utiliser `.replace("http://", "")`.

---

**Q : Ma table Bronze existe déjà, je veux rajouter des données sans écraser.**

R : Utiliser `mode("append")` à la place de `mode("overwrite")` :

```python
df.write.format("iceberg").mode("append").saveAsTable("nessie.bronze.ma_table")
```

```r
spark_write_table(df, "nessie.bronze.ma_table", mode = "append", format = "iceberg")
```

---

**Q : Comment voir toutes les tables disponibles ?**

R : En Python/Spark ou R/sparklyr :

```python
# Python
spark.sql("SHOW TABLES IN nessie.bronze").show()
spark.sql("SHOW TABLES IN nessie.silver").show()
```

```r
# R
DBI::dbGetQuery(sc, "SHOW TABLES IN nessie.bronze")
```

Ou ouvrir `exploration.ipynb` / `exploration.R` qui liste les tables au démarrage.

---

**Q : Je travaille en Stata, comment je récupère le résultat d'une table Silver ?**

R : Deux options :
1. Quelqu'un exporte la table Silver en CSV/Parquet vers `staging/` (depuis Python ou R), puis vous téléchargez avec `exploration.do`.
2. Si Trino ODBC est configuré sur votre poste : `odbc load, exec("SELECT * FROM silver.ma_table") dsn("Trino_ANSTAT") clear`.

---

**Q : Mon job Spark en production prend trop de temps / manque de mémoire.**

R : Augmenter les ressources dans le YAML :

```yaml
driver:
  memory: "16g"
executor:
  cores: 4
  instances: 4
  memory: "16g"
```

---

**Q : Comment annuler une transformation Silver qui contient une erreur ?**

R : Iceberg garde l'historique complet. En Spark :

```python
# Voir l'historique
spark.sql("SELECT * FROM nessie.silver.ma_table.snapshots").show()

# Revenir à un snapshot précédent (rollback)
spark.sql("CALL nessie.system.rollback_to_snapshot('silver.ma_table', <snapshot_id>)")
```

---

**Q : Quelle est la différence entre `staging` et `Bronze` ?**

R : `staging` est un bucket MinIO ordinaire — c'est juste un espace de fichiers, sans structure. `Bronze` est un ensemble de **tables Iceberg** : les données sont indexées, versionnées, interrogeables en SQL, et bénéficient du time travel. On passe de staging à Bronze via un job Spark.
