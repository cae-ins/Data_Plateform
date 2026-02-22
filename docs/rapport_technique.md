# Rapport Technique — Data Platform ANSTAT

**Institut National de la Statistique**
**Direction des Systèmes d'Information et des Données**

---

## Table des matières

1. [Contexte et objectifs](#1-contexte-et-objectifs)
2. [Architecture de la plateforme](#2-architecture-de-la-plateforme)
3. [Composants techniques](#3-composants-techniques)
4. [Organisation du dépôt](#4-organisation-du-dépôt)
5. [Fichiers de travail par langage](#5-fichiers-de-travail-par-langage)
6. [Templates réutilisables](#6-templates-réutilisables)
7. [Documentation utilisateur](#7-documentation-utilisateur)
8. [Proof of Concept — Panel Administratif 2015-2025](#8-proof-of-concept--panel-administratif-2015-2025)
9. [Patterns et conventions](#9-patterns-et-conventions)
10. [Perspectives](#10-perspectives)

---

## 1. Contexte et objectifs

### 1.1 Problématique initiale

Avant la mise en place de la Data Platform, les projets statistiques de l'ANSTAT présentaient des caractéristiques communes problématiques :

- **Silos de données** : chaque projet gérait ses données en local, sur des disques individuels non partagés.
- **Absence de traçabilité** : aucun historique des transformations appliquées aux données.
- **Hétérogénéité des outils** : R, Python et Stata coexistaient sans infrastructure commune.
- **Reproductibilité fragile** : refaire un calcul nécessitait de retrouver les bons fichiers dans les bons dossiers, avec les bonnes versions des scripts.
- **Collaboration difficile** : un résultat produit en R n'était pas immédiatement accessible à un utilisateur Python ou Stata.

### 1.2 Objectifs de la plateforme

La Data Platform répond à ces problèmes par quatre principes fondamentaux :

| Principe | Réponse technique |
|----------|-------------------|
| **Stockage centralisé** | MinIO — entrepôt objet S3 accessible depuis toutes les machines et tous les outils |
| **Versionnement des données** | Apache Iceberg + Project Nessie — chaque table est versionnée comme du code |
| **Standard de traitement** | Apache Spark — moteur de calcul distribué, compatible Python (PySpark), R (sparklyr) |
| **Automatisation** | Apache Airflow + Spark Operator — les pipelines validés en dev tournent en production sans réécriture |

### 1.3 Périmètre couvert

L'ANSTAT travaille avec trois langages statistiques en usage équilibré :

- **Python** : nowcasting, GeoAI, traitement CNPS, LLMs
- **R** : panel administratif, poids d'enquête ENE, IPM/CAE
- **Stata** : traitement déclaratif CNPS, CAE-IPM, ENE

La plateforme supporte les trois, avec des templates et des patterns adaptés à chaque écosystème.

---

## 2. Architecture de la plateforme

### 2.1 Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SOURCES DE DONNÉES                                   │
│  Excel · CSV · Stata .dta · Parquet · GeoTIFF · Sorties de modèles ML  │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  Upload manuel ou automatisé
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      MinIO — STAGING                                    │
│  Zone de dépôt intermédiaire. Accessible depuis toutes les machines.    │
│  Bucket : staging/                                                      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  Job Spark (notebook / script K8s)
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               MinIO — BRONZE · Tables Iceberg                           │
│  Données brutes ingérées. Tout en String. Aucune transformation métier. │
│  Versionnées par Nessie. Bucket : bronze/                               │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  Transformation (cast, nettoyage, règles)
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               MinIO — SILVER · Tables Iceberg                           │
│  Données propres, typées, déduplicées, enrichies, règles métier.        │
│  Source de vérité pour les analyses. Bucket : bronze/ (via Nessie)      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  Agrégation
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               MinIO — GOLD · Tables Iceberg                             │
│  Résultats agrégés. Prêts pour dashboards, exports, indicateurs.        │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                        ┌───────┴────────┐
                        ▼                ▼
                  Trino (SQL)      Exports CSV
                  Dashboards       staging/exports/
```

### 2.2 Principe dev / production

Le pattern central de la plateforme est la **séparation stricte entre logique métier et configuration infrastructure**.

**En développement** (notebook ou script local) :

```python
# Toute la configuration est dans le code
conf = SparkConf() \
    .set("spark.driver.memory", "16g") \
    .set("spark.jars.packages", "org.apache.iceberg:...") \
    .set("spark.sql.catalog.nessie.uri", NESSIE_URI) \
    # ...
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

**En production** (job Spark Operator sur Kubernetes) :

```python
# Le script ne contient que la logique
spark = SparkSession.builder.getOrCreate()
# Toute la config est portée par le YAML
```

La configuration (mémoire, packages, endpoints, credentials) est entièrement externalisée dans le fichier YAML du SparkApplication Kubernetes.

---

## 3. Composants techniques

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Spark | 3.5.0 | Moteur de calcul distribué |
| Apache Iceberg | 1.6.1 | Format de table ACID avec time travel |
| Project Nessie | 0.77.1 | Catalogue de données versionné (git pour les données) |
| MinIO | — | Stockage objet S3-compatible |
| Apache Airflow | — | Ordonnancement des pipelines |
| Spark Operator | — | Exécution des jobs Spark sur Kubernetes |
| Trino | — | Requêtage SQL léger sur les tables Iceberg |

### 3.1 Packages Spark (versions fixes)

```yaml
packages:
  - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1
  - org.apache.hadoop:hadoop-aws:3.3.4
  - org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1
  # Uniquement si source Excel :
  - com.crealytics:spark-excel_2.12:3.5.0_0.20.3
```

### 3.2 Endpoints

| Environnement | MinIO | Nessie |
|---------------|-------|--------|
| Machine locale | `http://192.168.1.230:30137` | `http://192.168.1.230:30604/api/v1` |
| JupyterHub (cluster) | `http://minio.mon-namespace.svc.cluster.local:80` | `http://nessie.trino.svc.cluster.local:19120/api/v1` |

Tous les fichiers de travail contiennent les deux blocs en commentaire — l'utilisateur active le bon selon son environnement.

---

## 4. Organisation du dépôt

```
Data_Plateform/
│
│  ── DOCUMENTATION
├── README.md                          Point d'entrée du dépôt
├── Passage_du_Brouillon_à_k8s.md      Guide dev → production
├── docs/
│   ├── guide_utilisateur.md           Guide complet (ce document complète)
│   └── rapport_technique.md           Ce document
│
│  ── FICHIERS DE TRAVAIL (Dev / Brouillon)
│
│   Python (Jupyter Notebook)
├── local-bronze.ipynb                 Fichier local → Bronze
├── staging-bronze.ipynb               MinIO staging → Bronze
├── bronze-silver.ipynb                Bronze → Silver
├── exploration.ipynb                  Requêtes / exploration
├── silver-gold.ipynb                  Silver → Gold
│
│   R (Script sparklyr)
├── local-bronze.R                     Fichier local → Bronze
├── staging-bronze.R                   MinIO staging → Bronze
├── bronze-silver.R                    Bronze → Silver
├── exploration.R                      Requêtes / exploration
├── silver-gold.R                      Silver → Gold
│
│   Stata (Do-file, Stata 16+)
├── local-bronze.do                    Fichier local → staging → Bronze
├── staging-bronze.do                  MinIO staging → traitement → staging
├── exploration.do                     Lecture depuis MinIO / Trino ODBC
│
│  ── PASSAGE EN PRODUCTION
├── SCRIPT_A_UPLOADER_SUR_MINIO.py     Modèle de script Spark minimal (K8s)
├── YAML_POUR_SPARK-OPERATOR.yaml      Modèle de SparkApplication K8s
├── dag_airflow.py                     Modèle de DAG Airflow
│
│  ── TEMPLATES RÉUTILISABLES
├── templates/
│   ├── .env.example                   Template credentials
│   ├── README.md                      Guide des templates
│   ├── python/                        Snippets Python standalone
│   ├── R/                             Snippets R (arrow + aws.s3)
│   └── spark/                         Scripts Spark + YAML K8s
│
│  ── PROOF OF CONCEPT
└── template_project/                  Exemple complet : panel_admin_data
    ├── README.md
    ├── 00_upload_sources_minio.R
    ├── 01_pre_analyse.R
    ├── 02_staging_to_bronze.R
    ├── 03_bronze_to_silver.R
    ├── 04_validation_silver.R
    └── 05_silver_to_gold.R
```

---

## 5. Fichiers de travail par langage

Chaque étape du pipeline dispose de trois implémentations équivalentes — une par langage. L'utilisateur choisit selon son environnement habituel.

### 5.1 Correspondance des fichiers

| Étape du pipeline | Python | R | Stata |
|-------------------|--------|---|-------|
| Fichier local → Bronze | `local-bronze.ipynb` | `local-bronze.R` | `local-bronze.do` |
| MinIO staging → Bronze | `staging-bronze.ipynb` | `staging-bronze.R` | `staging-bronze.do` |
| Bronze → Silver | `bronze-silver.ipynb` | `bronze-silver.R` | *(via Spark après dépôt)* |
| Exploration / requêtes | `exploration.ipynb` | `exploration.R` | `exploration.do` |
| Silver → Gold | `silver-gold.ipynb` | `silver-gold.R` | *(via Spark après dépôt)* |

### 5.2 Structure commune à tous les fichiers

Chaque fichier suit la même organisation en cinq sections :

```
1. CONFIGURATION    Deux blocs endpoints (local / JHub), activer le bon
2. SPARK SETUP      Configuration complète de la session Spark
3. CHARGEMENT       Lecture depuis fichier local, staging ou table Iceberg
4. TRAITEMENTS      Section vide balisée — l'utilisateur écrit sa logique
5. ÉCRITURE         Persistance vers la table Iceberg cible
```

Cette structure est identique en Python (SparkConf + SparkSession), R (spark_config + spark_connect) et Stata (globals + blocs `python:`).

### 5.3 Spécificité Stata

Stata ne dispose pas d'interface native pour Iceberg ou MinIO. Le flux Stata est donc :

```
MinIO staging
     │  Téléchargement via bloc python: (Stata 16+)
     ▼
Stata — traitements statistiques
     │  Upload du résultat via bloc python:
     ▼
MinIO staging
     │  Job Spark déclenché manuellement ou via Airflow
     ▼
Table Iceberg Bronze / Silver
```

Cette approche préserve l'intégralité des commandes Stata existantes. Python (intégré dans Stata) ne sert qu'au transfert de fichiers.

---

## 6. Templates réutilisables

Le dossier `templates/` contient des snippets autonomes pour les opérations d'import/export MinIO, utilisables indépendamment d'un projet complet.

### 6.1 Templates Python (standalone)

| Fichier | Usage |
|---------|-------|
| `python/01_read_excel_from_minio.py` | `minio-py` + `pandas` — télécharge `.xlsx` en mémoire |
| `python/02_read_parquet_from_minio.py` | `s3fs` + `pyarrow` — streaming direct, support Polars |
| `python/03_read_dta_from_minio.py` | `minio-py` + `pyreadstat` — préserve labels Stata |
| `python/04_read_csv_from_minio.py` | `minio-py` + `pandas` |
| `python/05_write_parquet_to_minio.py` | `s3fs` + `pyarrow` — compression snappy, partitionnement optionnel |
| `python/06_write_csv_to_minio.py` | `minio-py` + `pandas` — UTF-8 BOM pour Excel français |

### 6.2 Templates R

| Fichier | Usage |
|---------|-------|
| `R/01_read_from_minio.R` | 4 options : Parquet (`arrow` S3), CSV, DTA, Excel via `aws.s3` |
| `R/02_write_to_minio.R` | 3 options : Parquet (`arrow`), CSV (`fwrite`), DTA (`haven`) |

### 6.3 Templates Spark (production K8s)

#### Scripts Python

| Fichier | Usage |
|---------|-------|
| `spark/01_excel_to_bronze.py` | Excel staging → Iceberg Bronze (avec spark-excel) |
| `spark/02_parquet_to_bronze.py` | Parquet staging → Iceberg Bronze |
| `spark/03_dta_to_bronze.py` | Stata DTA → Iceberg Bronze (pont pandas via pyreadstat) |
| `spark/04_bronze_to_silver.py` | Bronze → Silver (transformations SQL) |

#### YAML SparkApplication

| Fichier | Usage |
|---------|-------|
| `yaml/00_secret_credentials.yaml` | Kubernetes Secret — credentials MinIO (à appliquer une fois) |
| `yaml/01_excel_to_bronze.yaml` | Job avec spark-excel (driver 1c/16g, 2 exec 2c/8g) |
| `yaml/02_parquet_or_dta_to_bronze.yaml` | Job sans spark-excel (plus léger) |
| `yaml/03_bronze_to_silver.yaml` | Job transformation Silver (driver 2c/16g, 3 exec 4c/8g) |

#### Amélioration sécurité apportée

Les YAML de production utilisent des `secretKeyRef` pointant vers un Kubernetes Secret, à la place des credentials en clair présents dans le YAML original :

```yaml
# Avant (dans YAML_POUR_SPARK-OPERATOR.yaml original)
env:
  - name: AWS_ACCESS_KEY_ID
    value: "datalab-team"          # ← credentials en clair

# Après (dans les YAML templates)
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: minio-credentials
        key: access-key            # ← référence au Secret K8s
```

---

## 7. Documentation utilisateur

### 7.1 README.md

Point d'entrée du dépôt. Contient :
- Tableau de correspondance fichier/étape/langage
- Schéma ASCII de l'architecture
- Tableau des endpoints par environnement
- Liens vers les guides détaillés

### 7.2 Guide utilisateur (`docs/guide_utilisateur.md`)

Document principal destiné à tout utilisateur souhaitant travailler avec la plateforme. Structure en 10 sections :

1. **Pourquoi cette plateforme** — valeur ajoutée face aux silos
2. **Architecture et concepts** — schéma complet, tableau des composants avec analogies
3. **Accès et prérequis** — endpoints, credentials, packages par langage
4. **Flux de travail standard** — les 3 étapes, quel fichier ouvrir dans quelle situation
5. **Déposer ses données** — console web, Python, R, formats acceptés
6. **Travailler sur les données** — guide pas à pas par langage (Python, R, Stata)
7. **Les couches Bronze/Silver/Gold** — définition, règles, time travel
8. **Passer en production** — étapes concrètes, commandes kubectl
9. **Bonnes pratiques** — nommage, organisation staging, formats, mémoire Spark
10. **FAQ** — 8 questions/réponses avec code

### 7.3 Passage du Brouillon à K8s (`Passage_du_Brouillon_à_k8s.md`)

Document technique expliquant le passage d'un notebook de développement vers un job Spark Operator en production. Inclut le tableau complet des fichiers de travail (Python, R, Stata) et les 5 étapes de déploiement.

---

## 8. Proof of Concept — Panel Administratif 2015-2025

### 8.1 Contexte du projet original

Le projet `panel_admin_data` est le plus grand projet de données de l'ANSTAT. Il consolide les fichiers de solde mensuels de la fonction publique sur 10 ans.

**Caractéristiques du projet original** :

| Paramètre | Valeur |
|-----------|--------|
| Fichiers source | ~120 fichiers Excel mensuels (2015–2025) |
| Structure source | 2 feuilles par fichier (F1 : données agents, F2 : codes numériques) |
| Résultat | `base_consolidee_2015_2025.parquet` |
| Dimensions | **31 871 913 lignes × 425 colonnes** |
| Colonnes standard | 26 variables harmonisées via mapping regex |
| Codes ANSTAT | 8 dictionnaires (organisme, grade, emploi, service, fonction, poste, affectation, position solde) |
| Langage original | R (data.table, readxl, stringi, arrow) |

**Scripts originaux** (dans `panel_admin_data/Script_final_R/`) :

| Script | Rôle |
|--------|------|
| `01_pre_analyse_colonnes_2015_2025.R` | Inventaire des colonnes, détection des changements de structure année par année |
| `02_consolidation_complete_2015_2025.R` | Pipeline complet : lecture → mapping → fusion F1+F2 → enrichissement → export Parquet |
| `03_post_validation_base_finale.R` | Contrôles qualité : NA suspects, doublons, cohérence montants, distribution situations |

### 8.2 Architecture de la solution PoC

Le PoC traduit ce projet en 5 scripts R utilisant les templates de la Data Platform :

```
staging/panel_admin/
  fichiers_mensuels/    ← ~120 Excel déposés par 00_upload_sources_minio.R
  references/           ← FICHIER_ANSTAT_CODE_2025.xlsx
  pre_analyse/          ← Rapports CSV de 01_pre_analyse.R
  validation/           ← Rapports QC de 04_validation_silver.R
  exports_gold/         ← CSV Gold de 05_silver_to_gold.R
        │
        ▼ 02_staging_to_bronze.R (batch 12 fichiers)
nessie.bronze.panel_admin_solde_mensuel
        │
        ▼ 03_bronze_to_silver.R
nessie.silver.panel_admin_solde_mensuel
        │
        ├──▶ nessie.gold.panel_admin_masse_salariale
        └──▶ nessie.gold.panel_admin_effectifs
```

### 8.3 Description des scripts PoC

#### `00_upload_sources_minio.R`

Upload des fichiers sources depuis le disque local vers MinIO staging. Opération réalisée une seule fois.

- Parcourt récursivement le dossier local des Excel mensuels
- Uploade chaque fichier vers `staging/panel_admin/fichiers_mensuels/`
- Uploade `FICHIER_ANSTAT_CODE_2025.xlsx` vers `staging/panel_admin/references/`
- Utilise `aws.s3::put_object()` avec gestion des erreurs par fichier

#### `01_pre_analyse.R`

Adapté de `01_pre_analyse_colonnes_2015_2025.R`. Lit les en-têtes des Excel depuis staging sans télécharger les données complètes.

- Liste les objets MinIO dans `staging/panel_admin/fichiers_mensuels/`
- Pour chaque fichier : téléchargement temporaire, `read_excel(..., n_max = 0)`, suppression
- Détecte les changements de colonnes entre périodes consécutives
- Produit trois rapports CSV déposés dans `staging/panel_admin/pre_analyse/`

#### `02_staging_to_bronze.R`

Cœur de l'ingestion. Adapté de la partie lecture du script 02 original.

**Architecture batch** :
- Récupère la liste des objets dans `staging/panel_admin/fichiers_mensuels/`
- Divise en lots de 12 fichiers (= 1 an)
- Pour chaque lot :
  1. Téléchargement de chaque Excel en fichier temporaire
  2. Détection automatique de la ligne d'en-tête (`detecter_entete()`)
  3. Lecture F1 (agents) et F2 (codes numériques) avec `col_types = "text"` (tout String pour Bronze)
  4. Normalisation minimale des noms de colonnes
  5. Fusion F1 + F2 sur la clé `MATRICULE||CODE_ORGANISME`
  6. Ajout des métadonnées `PERIODE` et `FICHIER_SOURCE`
  7. `rbindlist()` des 12 mois
  8. `copy_to()` → `spark_write_table(..., mode = "append", format = "iceberg")`

**Règle Bronze** : aucune transformation métier — tout est conservé en `character`, aucun filtrage.

#### `03_bronze_to_silver.R`

Adapté de la partie enrichissement du script 02 original. Toutes les fonctions métier sont préservées à l'identique.

**Fonctions métier réutilisées** :

```r
normaliser_pour_matching(texte)
# Normalisation pour le matching fuzzy :
# suppression accents, ponctuation, majuscules, espaces multiples
# ex. "Ministère de l'Économie" → "MINISTERE DE L ECONOMIE"

mapper_colonnes(noms_colonnes)
# 26 patterns regex pour mapper les noms bruts vers les noms standard
# ex. "MONTANT BRUT", "SALAIRE BRUT", "REMUNERATION BRUTE" → "montant_brut"

normaliser_situation(situation)
# Catégorise les situations administratives en 3 groupes valides :
# "en_activite", "regul_indemnites", "demi_solde"
# Toutes les autres → "autre" (filtrées en Silver)
```

**Processus Silver** :
1. Collect de la table Bronze en `data.table` (driver R)
2. Application de `mapper_colonnes()` sur les noms de colonnes Bronze
3. Création de `situation_brute` et `situation_normalisee`
4. Filtrage : conservation des situations valides uniquement
5. Chargement des tables de codes depuis `staging/panel_admin/references/`
6. Enrichissement par jointure fuzzy pour 8 codes ANSTAT
7. Cast des colonnes montants en `numeric`
8. `copy_to()` → `spark_write_table(..., mode = "overwrite", format = "iceberg")`

**Taux de matching attendus** (observés sur le projet original) :

| Code | Taux typique |
|------|-------------|
| `CODE_ORGANISME` | > 90% |
| `CODE_GRADE` | > 85% |
| `CODE_EMPLOI` | > 80% |
| `CODE_SERVICE` | ~70% |

#### `04_validation_silver.R`

Adapté de `03_post_validation_base_finale.R`. Les contrôles sont exprimés en SQL Spark et s'exécutent directement sur la table Silver sans télécharger les 31M de lignes.

| Contrôle | Méthode |
|----------|---------|
| Complétude colonnes clés | `COUNT(col) / COUNT(*)` par colonne via `DBI::dbGetQuery()` |
| Variations NA par année | `SUM(CASE WHEN col IS NULL ...)` groupé par `SUBSTR(periode, 1, 4)` |
| Doublons matricule × période | `GROUP BY matricule, periode HAVING COUNT(*) > 1` |
| Cohérence montants | `WHERE montant_net > montant_brut` |
| Distribution situations | `GROUP BY situation_normalisee` |

Les rapports sont sauvegardés sous forme de CSV dans `staging/panel_admin/validation/`.

#### `05_silver_to_gold.R`

Produit deux tables Gold par agrégation SQL directe sur Silver.

**Table `nessie.gold.panel_admin_masse_salariale`** :

```sql
SELECT
    periode, annee, mois,
    code_organisme, organisme, situation_normalisee,
    COUNT(*) AS nb_lignes,
    COUNT(DISTINCT matricule) AS nb_agents_uniques,
    ROUND(SUM(montant_brut), 0) AS masse_brute,
    ROUND(SUM(montant_net), 0) AS masse_nette,
    ROUND(AVG(montant_brut), 0) AS salaire_brut_moyen,
    ROUND(PERCENTILE_APPROX(montant_brut, 0.5), 0) AS salaire_brut_mediane
FROM nessie.silver.panel_admin_solde_mensuel
GROUP BY periode, annee, mois, code_organisme, organisme, situation_normalisee
```

**Table `nessie.gold.panel_admin_effectifs`** :

```sql
SELECT
    periode, annee, mois, situation_normalisee, code_organisme,
    COUNT(*) AS nb_lignes,
    COUNT(DISTINCT matricule) AS nb_agents_uniques,
    SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END) AS nb_hommes,
    SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END) AS nb_femmes
FROM nessie.silver.panel_admin_solde_mensuel
GROUP BY periode, annee, mois, situation_normalisee, code_organisme
```

Les deux tables sont également exportées en CSV vers `staging/panel_admin/exports_gold/`.

### 8.4 Ce que démontre le PoC

| Capacité démontrée | Comment |
|--------------------|---------|
| Ingestion de sources hétérogènes | 120 Excel avec structures variables sur 10 ans |
| Préservation de la logique métier R | Fonctions `mapper_colonnes`, `normaliser_situation`, `enrichir_par_codes` réutilisées à l'identique |
| Traitement batch à grande échelle | 31M lignes traités par lots de 12 mois |
| Versionnement des données | Toute modification Bronze/Silver/Gold est tracée par Nessie |
| Validation par SQL distribué | Contrôles QC sans télécharger la table complète |
| Séparation Bronze / Silver / Gold | Données brutes, propres, et agrégées dans trois couches distinctes |
| Export multi-format | Tables Iceberg + CSV staging pour la diffusion |

---

## 9. Patterns et conventions

### 9.1 Nommage des tables Iceberg

```
nessie.<couche>.<projet>_<description>

Exemples valides :
  nessie.bronze.panel_admin_solde_mensuel
  nessie.silver.cnps_declarations_nettoyees
  nessie.gold.ene_taux_emploi_trimestriel
  nessie.gold.panel_admin_masse_salariale
```

Règles :
- Minuscules uniquement
- Séparateur `_` (pas de tirets ni d'espaces)
- Pas d'accents, pas de caractères spéciaux
- Préfixer par le nom du projet

### 9.2 Organisation dans MinIO staging

```
staging/
  <projet>/
    fichiers_mensuels/      ← données sources
    references/             ← tables de codes, référentiels
    pre_analyse/            ← rapports d'analyse exploratoire
    validation/             ← rapports de contrôle qualité
    exports_gold/           ← CSV pour diffusion externe
```

### 9.3 Règles par couche

| Couche | Règle | Exemple |
|--------|-------|---------|
| **Bronze** | Tout en String, aucun filtrage, aucune transformation métier | `montant_brut = "12500,00"` (String) |
| **Silver** | Types corrects, filtrage validité, règles métier appliquées | `montant_brut = 12500.00` (Double) |
| **Gold** | Dénormalisé, pré-agrégé pour un usage précis | `masse_brute = 1245000000` par période × organisme |

### 9.4 Gestion des credentials

| Contexte | Méthode |
|----------|---------|
| Développement local | Fichier `.env` (jamais commité) + `python-dotenv` / `dotenv` R |
| JupyterHub | Variables d'environnement injectées par l'administrateur |
| Production K8s | Kubernetes Secret `minio-credentials` + `secretKeyRef` dans les YAML |

### 9.5 Formats de données recommandés

| Usage | Format | Justification |
|-------|--------|---------------|
| Stocker des résultats R/Python | Parquet (snappy) | Compact, rapide, compatible tous outils |
| Partager avec Excel | CSV (`;`, UTF-8 BOM) | Compatible Excel français |
| Partager avec Stata | DTA version 15 | Préserve les labels de variables |
| Tables plateforme | Iceberg | ACID, time travel, SQL distribué |

### 9.6 Dimensionnement Spark

| Volume | Driver | Executor | Instances |
|--------|--------|----------|-----------|
| < 500 Mo | 4g | 4g | 2 |
| 500 Mo – 5 Go | 8g | 4g | 2 |
| 5 – 30 Go | 16g | 8g | 2 |
| > 30 Go (ex. panel_admin 31M) | 16g | 16g | 4 |

---

## 10. Perspectives

### 10.1 Extensions prioritaires

**Autres projets à intégrer comme exemples**

Le PoC panel_admin_data est le premier dossier exemple. D'autres projets de l'ANSTAT ont vocation à rejoindre `template_project/` :

| Projet | Langage principal | Spécificité technique |
|--------|-------------------|-----------------------|
| `cnps_treatment` | Python + R + Stata | GLM logistique, IPW, règles de Rubin |
| `ENE_SURVEY_WEIGHTS` | R | Calibration ReGenesees, 156 contraintes |
| `cae_ipm` | Stata + R | IPM PNUD, données RGPH 2021 |
| `gdp_nowcasting` | Python | 8 modèles ML, TimeSeriesSplit OOS |
| `access_remote_` | Python (PyTorch) | Tenseurs GPU, décroissance logistique |

**Trino ODBC pour Stata**

Configurer et documenter la connexion Trino ODBC pour permettre à Stata d'interroger directement les tables Silver/Gold sans passer par Python.

**Templates GPU**

Créer des fichiers de travail pour les jobs PyTorch (GeoAI U-Net, IAI tenseurs) avec configuration GPU dans les YAML Spark Operator.

### 10.2 Améliorations infrastructure

- **Gestion des secrets** : migrer vers Vault ou External Secrets Operator pour éviter les secrets K8s statiques.
- **Monitoring** : intégrer des métriques Spark (Prometheus/Grafana) pour suivre les performances des jobs.
- **CI/CD pipelines** : automatiser la validation des scripts via des tests unitaires déclenchés à chaque commit.

### 10.3 Gouvernance des données

- Définir un catalogue de données formalisé (descriptions des tables, responsables, SLA de fraîcheur).
- Mettre en place des politiques de rétention par couche (Bronze : 5 ans, Silver : illimité, Gold : selon usage).
- Documenter les lignages de données (quelle table Gold dépend de quelle table Silver, etc.).

