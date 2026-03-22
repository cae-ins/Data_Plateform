---
marp: true
theme: default
paginate: true
header: "PoC panel_admin_data — Data Platform"
footer: "Direction des Systèmes d'Information Statistique"
---

# Proof of Concept
## `panel_admin_data` sur la Data Platform

---

## Pourquoi ce PoC ?

- `panel_admin_data` est le **premier projet pilote** de la Data Platform
- Il illustre concrètement le workflow complet : staging → bronze → silver → gold
- Il sert de **référence et de démonstration** pour les équipes

> **Objectif** : montrer qu'un projet R existant peut être migré sur la plateforme sans réécrire toute la logique métier

---

## Contexte du Projet

- **Source** : ~120 fichiers Excel mensuels, 2015–2025
  - Feuille F1 : données de paie
  - Feuille F2 : codes numériques
- **Volume final** : **31 871 913 lignes × 425 colonnes**
- **Format de sortie actuel** : Parquet (snappy) + CSV
- **Langage** : R (data.table, arrow)

---

## Le Défi Technique

Les fichiers Excel mensuels ne sont **pas uniformes** :

- Les noms de colonnes varient d'un mois à l'autre
- Des codes numériques changent de signification selon la période
- La consolidation nécessite un **mapping regex** et une **normalisation** des identifiants

→ La logique métier existante est précieuse et doit être préservée

---

## Scripts Originaux (Legacy)

| Script | Rôle |
|--------|------|
| `01_pre_analyse_colonnes_2015_2025.R` | Inventaire des colonnes des 120 fichiers Excel |
| `02_consolidation_complete_2015_2025.R` | `mapper_colonnes()` + `normaliser_pour_matching()`, batch par 12 mois |
| `03_post_validation_base_finale.R` | Contrôles QC : NA suspects, doublons, montants, situations |

Ces scripts sont conservés dans `legacy/` à titre de référence.

---

## Architecture Cible du PoC

```
examples/panel_admin_data/
├── README.md
├── 00_upload_sources_staging.R     ← Dépôt Excel → MinIO staging
├── 01_staging_to_bronze.R          ← Ingestion Excel → Bronze Iceberg
├── 02_bronze_to_silver.R           ← Mapping colonnes + normalisation → Silver
├── 03_exploration_validation.R     ← Contrôles QC + requêtes
├── 04_silver_to_gold.R             ← Agrégations → Gold
└── legacy/
    ├── 01_pre_analyse_colonnes_2015_2025.R
    ├── 02_consolidation_complete_2015_2025.R
    └── 03_post_validation_base_finale.R
```

---

## Workflow du PoC — Vue d'ensemble

```
Fichiers Excel mensuels (2015–2025)
         ↓  00_upload_sources_staging.R
    MinIO : staging/
         ↓  01_staging_to_bronze.R
    Table Iceberg : Bronze  (données brutes)
         ↓  02_bronze_to_silver.R
    Table Iceberg : Silver  (colonnes mappées, identifiants normalisés)
         ↓  03_exploration_validation.R
    Contrôles QC  (NA, doublons, montants)
         ↓  04_silver_to_gold.R
    Table Iceberg : Gold    (masse salariale par période / organisme)
```

---

## Étape 0 — Upload vers MinIO Staging

**Script** : `00_upload_sources_staging.R`

- Utilise le template `02_write_to_minio.R` de la plateforme
- Dépose les 120 fichiers Excel dans `staging/panel_admin_data/`
- Conserve l'arborescence par année/mois

```r
# Exemple (adapté du template)
s3_upload(
  local_path = "data/Excel/2024/01_jan.xlsx",
  bucket     = "staging",
  s3_path    = "panel_admin_data/2024/01_jan.xlsx"
)
```

---

## Étape 1 — Staging → Bronze

**Script** : `01_staging_to_bronze.R`

- Lit chaque fichier Excel depuis MinIO staging (feuille F1)
- Ingère **sans transformation** dans une table Iceberg Bronze
- Conserve toutes les colonnes originales + métadonnées (nom fichier, date ingestion)

```r
# Lecture Excel depuis staging
df_raw <- read_excel_from_minio(bucket = "staging",
                                path   = "panel_admin_data/2024/01_jan.xlsx",
                                sheet  = "F1")
# Écriture Bronze
write_iceberg(df_raw, table = "bronze.panel_admin_data",
              mode = "append")
```

---

## Étape 2 — Bronze → Silver

**Script** : `02_bronze_to_silver.R`

Reprend la logique de `02_consolidation_complete_2015_2025.R` :

- `mapper_colonnes()` : mapping regex des noms de colonnes variables
- `normaliser_pour_matching()` : normalisation des identifiants
- Traitement par **batch de 12 mois** pour maîtriser la mémoire
- Output : **31M lignes × 425 colonnes** dans la table Silver

---

## Étape 3 — Exploration & Validation

**Script** : `03_exploration_validation.R`

Reprend la logique de `03_post_validation_base_finale.R` :

| Contrôle | Description |
|----------|-------------|
| NA suspects | Champs clés vides sur volumes anormaux |
| Doublons | Identifiants répétés sur même période |
| Montants | Valeurs aberrantes (négatifs, extrêmes) |
| Situations | Cohérence codes situation administrative |

→ Requêtes directes sur la table Silver Iceberg via `arrow` / `dplyr`

---

## Étape 4 — Silver → Gold

**Script** : `04_silver_to_gold.R`

Exemple d'agrégations produites :

- Masse salariale totale par **période** (mois/année)
- Masse salariale par **organisme employeur**
- Effectifs par **catégorie / grade**
- Évolution mensuelle des montants

→ Table Gold légère, directement consommable pour le reporting et la BI

---

## Ce que le PoC Démontre

| Capacité | Démontré par |
|----------|-------------|
| Ingestion de fichiers hétérogènes | Étape 01 : Excel → Bronze |
| Consolidation complexe avec logique métier | Étape 02 : mapping regex, normalisation |
| Contrôle qualité à grande échelle | Étape 03 : QC sur 31M lignes |
| Agrégation et production d'indicateurs | Étape 04 : Gold |
| Versionnement des tables | Nessie sur chaque couche |
| Réutilisabilité du code existant | Scripts legacy préservés et réutilisés |

---

## Comparaison : Avant / Après

| | Avant (legacy) | Après (PoC Data Platform) |
|---|---|---|
| **Exécution** | Local, machine unique | Distribué sur K8s |
| **Stockage** | Fichiers locaux | MinIO (haute dispo) |
| **Versionnement** | Git sur scripts | Git scripts + Nessie tables |
| **Reproductibilité** | Manuelle | Automatisée (Airflow) |
| **Consommation** | CSV/Parquet local | Table Iceberg interrogeable |

---

## Prochaines Étapes

1. Lecture des 3 scripts legacy pour extraction de la logique métier
2. Création de `00_upload_sources_staging.R` (dépôt des Excel)
3. Adaptation de l'ingestion dans `01_staging_to_bronze.R`
4. Portage de `mapper_colonnes()` dans `02_bronze_to_silver.R`
5. Portage des contrôles QC dans `03_exploration_validation.R`
6. Définition des agrégations Gold avec les experts métier

---

# Questions ?

> **Dépôt** : `examples/panel_admin_data/`
> **Scripts legacy** : `examples/panel_admin_data/legacy/`
> **Templates de base** : `templates/R/`
