Pour passer du brouillon à une exécution via le Spark Operator, il y a un principe et une structure technique à respecter : on sépare le code (logique métier) de la configuration (infrastructure).

---

## Les fichiers de travail (Brouillon / Dev)

Chaque fichier couvre une étape du pipeline. Trois langages disponibles — choisir selon votre environnement habituel.

### Python (Jupyter Notebook — JupyterHub)

| Notebook | Rôle |
|----------|------|
| `local-bronze.ipynb` | Charger un fichier local → table Iceberg Bronze |
| `staging-bronze.ipynb` | Charger un fichier depuis MinIO staging → table Iceberg Bronze |
| `bronze-silver.ipynb` | Lire Bronze → transformer → écrire Silver |
| `exploration.ipynb` | Requêter les tables Iceberg (Bronze/Silver), explorer, visualiser |
| `silver-gold.ipynb` | Agréger Silver → écrire Gold, exporter CSV |

### R (Script R — RStudio / JupyterHub)

| Script | Rôle |
|--------|------|
| `local-bronze.R` | Charger un fichier local → table Iceberg Bronze (sparklyr) |
| `staging-bronze.R` | Charger un fichier depuis MinIO staging → table Iceberg Bronze (sparklyr) |
| `bronze-silver.R` | Lire Bronze → transformer en dplyr/SQL → écrire Silver (sparklyr) |
| `exploration.R` | Requêter les tables Iceberg depuis R (sparklyr ou Trino/DBI) |
| `silver-gold.R` | Agréger Silver → écrire Gold, exporter CSV vers staging (sparklyr) |

### Stata (Do-file — Stata 16+)

| Do-file | Rôle |
|---------|------|
| `local-bronze.do` | Charger un fichier local, traiter en Stata, déposer sur staging via Python intégré |
| `staging-bronze.do` | Télécharger depuis MinIO, traiter en Stata, redéposer sur staging |
| `exploration.do` | Charger un export depuis MinIO ou requêter via Trino ODBC |

> **Note Stata** : Stata ne se connecte pas directement à Iceberg/Nessie.
> Le workflow est : MinIO staging → Stata (traitements) → MinIO staging → job Spark → Bronze/Silver.

---

## Passage en production (Brouillon → Spark Operator)

Dans Kubernetes, c'est le Spark Operator qui gère l'allocation de la mémoire et les paramètres réseau. On doit donc alléger le script pour qu'il soit portable.

**Ce qu'il faut enlever du script (déjà géré par le YAML) :**

- `spark.driver.host` et `bindAddress` (Kubernetes s'en occupe)
- `spark.driver.memory` (défini dans le YAML)
- `spark.jars.packages` (défini dans le YAML, téléchargés avant le lancement)

**Étapes :**
1. Copier la logique du notebook dans `templates/spark/0X_mon_job.py`
2. Remplacer `SparkSession.builder.config(conf=conf).getOrCreate()` par `SparkSession.builder.getOrCreate()`
3. Uploader le script sur MinIO : `s3a://sparkapplication/mon_job.py`
4. Adapter le YAML correspondant dans `templates/spark/yaml/`
5. Déclencher via `kubectl apply` ou via le DAG Airflow (`dag_airflow.py`)

NB : le fichier `staging-bronze.ipynb` est l'exemple de référence pour ce passage.
Les autres notebooks (`bronze-silver`, `silver-gold`) suivent le même principe.

