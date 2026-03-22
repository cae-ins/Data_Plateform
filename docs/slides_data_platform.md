---
marp: true
theme: default
paginate: true
header: "Data Platform — Architecture & Stack"
footer: "Direction des Systèmes d'Information Statistique"
---

# Data Platform
## Architecture, Stack et Usages

---

## Contexte et Objectifs

- **Problème** : données sources dispersées (Excel, Parquet, DTA, CSV), volumétries croissantes, pas de traçabilité
- **Solution** : une plateforme centralisée, versionnée et orchestrée
- **Objectifs**
  - Unifier l'ingestion et le traitement des données
  - Garantir la reproductibilité et la traçabilité
  - Permettre l'exploration et l'agrégation à grande échelle

---

## Architecture Globale

```
Données sources         Ingestion          Traitement          Consommation
(Excel, CSV, DTA)  →   Bronze (raw)   →   Silver (clean)  →   Gold (agrégé)
                        ↑                   ↑                   ↑
                        MinIO               Spark + Iceberg     Requêtes / BI
                        Staging             Nessie (catalogue)  Airflow
```

- Chaque couche est une **table Iceberg versionnée**
- Le catalogue Nessie permet un **contrôle de version git-like** sur les tables

---

## Stack Technique

| Composant | Rôle | Version |
|-----------|------|---------|
| **Apache Spark** | Traitement distribué | 3.5.0 |
| **Apache Iceberg** | Format de table ACID | 1.6.1 |
| **Nessie** | Catalogue versionné | 0.77.1 |
| **MinIO** | Stockage objet S3-compatible | — |
| **Apache Airflow** | Orchestration des DAGs | — |
| **Kubernetes** | Déploiement et scaling | — |

---

## Stockage : MinIO

**Object Store S3-compatible** — accessible depuis R, Python, Spark

| Bucket | Contenu |
|--------|---------|
| `staging` | Fichiers sources bruts déposés par les équipes |
| `bronze` | Données ingérées, non transformées |
| `silver` | Données nettoyées et consolidées |
| `gold` | Agrégations et indicateurs |
| `sparkapplication` | JARs et scripts Spark |

---

## Catalogue : Nessie

- Versionnement **git-like** des tables Iceberg
- Chaque transformation peut s'exécuter sur une **branche isolée**
- Merge vers `main` après validation
- Compatible avec Spark, Trino, Dremio

```
nessie/
├── main (production)
├── feature/nouvelle-ingestion
└── experiment/test-silver-v2
```

---

## Orchestration : Airflow + Kubernetes

- Les jobs Spark tournent comme des **SparkApplication** K8s
- Airflow pilote via `SparkKubernetesOperator` + `SparkKubernetesSensor`
- Architecture driver/executors gérée par le **Spark Operator**

```
Airflow DAG
  └── SparkKubernetesOperator (soumet le job)
        └── SparkApplication (K8s)
              ├── Driver  : 1 CPU / 16 GB
              └── Executor × 2 : 2 CPU / 8 GB chacun
```

---

## Gestion des Credentials

| Environnement | Méthode |
|---------------|---------|
| **Dev local** | Fichier `.env` (python-dotenv / dotenv R) — jamais commité |
| **Production K8s** | `Secret` Kubernetes `minio-credentials` → `secretKeyRef` dans les YAML |

- Séparation stricte **code / configuration**
- Le script Python/R ne contient aucune credential en dur

---

## Templates Disponibles

### Python (6 templates)
`01_read_excel` · `02_read_parquet` · `03_read_dta` · `04_read_csv` · `05_write_parquet` · `06_write_csv`

### R (2 templates)
`01_read_from_minio.R` · `02_write_to_minio.R`

### Spark / K8s (4 scripts + 4 YAML)
`01_excel_to_bronze` · `02_parquet_to_bronze` · `03_dta_to_bronze` · `04_bronze_to_silver`

---

## Projets Utilisateurs — Exemples

| Projet | Langage | Volumétrie / Particularité |
|--------|---------|---------------------------|
| `panel_admin_data` | R | 31M lignes × 425 cols, Parquet snappy |
| `gdp_nowcasting` | Python | 55 vars, 8 modèles ML, TimeSeriesSplit |
| `ENE_SURVEY_WEIGHTS` | R | 156 contraintes calibration, ReGenesees |
| `worspace_geo_ai` | Python | GeoTIFF 4 bandes, U-Net/ResNet34 |
| `vLLM_Deploy` | Python | LoRA Phi-3.5-mini, FAISS RAG, vLLM |
| `access_remote_` | Python | PyTorch, décroissance logistique |

---

## Workflow Type d'un Projet

```
1. Déposer les sources sur MinIO staging
      ↓
2. staging → Bronze (ingestion brute, Iceberg)
      ↓
3. Bronze → Silver (nettoyage, consolidation)
      ↓
4. Silver → Gold (agrégations, indicateurs)
      ↓
5. Exploration / validation (notebook ou script)
```

Chaque étape = **un script** + **un YAML SparkApplication**

---

## Bénéfices Clés

- **Reproductibilité** : chaque table est versionnée dans Nessie
- **Scalabilité** : Spark distribué sur K8s, auto-scaling des executors
- **Interopérabilité** : lecture depuis R, Python, Spark, Trino
- **Simplicité d'usage** : templates prêts à l'emploi, sections `# À MODIFIER` clairement balisées
- **Sécurité** : credentials isolés dans des Secrets K8s, jamais dans le code

---

# Questions ?

> **Dépôt** : `Data_Plateform/`
> **Documentation** : `docs/guide_utilisateur.md` · `docs/rapport_technique.md`
> **Templates** : `templates/`
