# Template Projet — Python

## Objectif

Ce dossier est un **template générique** pour intégrer un projet Python dans la Data Platform ANSTAT.
Il montre comment connecter n'importe quelle source de données au pipeline
MinIO → Bronze → Silver → Gold avec versionnement Iceberg/Nessie.

Copier ce dossier, renommer `MON_PROJET` par le nom de votre projet,
et adapter les sections balisées `# <<< À MODIFIER >>>`.

---

## Ordre d'exécution

```
00_upload_sources_minio.py   ← une seule fois (dépôt des sources)
        │
        ▼
01_pre_analyse.py            ← optionnel (exploration des fichiers staging)
        │
        ▼
02_staging_to_bronze.py      ← ingestion fichiers → Bronze Iceberg
        │
        ▼
03_bronze_to_silver.py       ← transformation → Silver Iceberg
        │
        ▼
04_validation_silver.py      ← contrôle qualité Silver
        │
        ▼
05_silver_to_gold.py         ← agrégations → Gold Iceberg
```

---

## Prérequis

```
pip install boto3 pandas openpyxl python-dotenv pyspark pyarrow
```

Fichier `.env` à la racine du projet (ne jamais committer) :
```
MINIO_ENDPOINT=http://192.168.1.230:30137
MINIO_ACCESS_KEY=datalab-team
MINIO_SECRET_KEY=minio-datalabteam123
NESSIE_URI=http://192.168.1.230:30604/api/v1
```

---

## Tables produites

| Table | Couche | Description |
|-------|--------|-------------|
| `nessie.bronze.mon_projet` | Bronze | Données brutes, tout String |
| `nessie.silver.mon_projet` | Silver | Colonnes typées, transformations métier |
| `nessie.gold.mon_projet_agreg` | Gold | Agrégations prêtes pour analyse |

---

## Structure MinIO

```
staging/
  mon_projet/
    sources/          ← fichiers sources déposés par 00_upload
    pre_analyse/      ← rapports produits par 01_pre_analyse
    validation/       ← rapports produits par 04_validation
    exports_gold/     ← CSV exportés par 05_silver_to_gold
```
