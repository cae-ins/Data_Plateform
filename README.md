# Data Platform — ANSTAT

Plateforme centralisée de stockage, traitement et consultation des données statistiques de l'ANSTAT.

---

## Démarrage rapide

1. **Lire** → [`docs/guide_utilisateur.md`](docs/guide_utilisateur.md) — guide complet pour tout utilisateur
2. **Choisir** un fichier de travail à la racine du dépôt selon votre langage et votre étape
3. **Configurer** vos accès : copier `.env.example` → `.env`, renseigner vos identifiants

---

## Les fichiers de travail

| Étape | Python (Notebook) | R (Script) | Stata (Do-file) |
|-------|-------------------|------------|-----------------|
| Fichier local → Bronze | `local-bronze.ipynb` | `local-bronze.R` | `local-bronze.do` |
| MinIO staging → Bronze | `staging-bronze.ipynb` | `staging-bronze.R` | `staging-bronze.do` |
| Bronze → Silver | `bronze-silver.ipynb` | `bronze-silver.R` | *(via Spark)* |
| Explorer les données | `exploration.ipynb` | `exploration.R` | `exploration.do` |
| Silver → Gold | `silver-gold.ipynb` | `silver-gold.R` | *(via Spark)* |

---

## Architecture en un coup d'œil

```
Vos données (Excel, CSV, DTA, Parquet)
        │
        ▼
  [ MinIO staging ]  ←── zone de dépôt intermédiaire (S3-compatible, on-premise)
        │
        ▼ job Spark (local ou Kubernetes)
  [ Bronze Iceberg ]  ←── données brutes, versionnées, immuables
        │
        ▼ transformation Spark (règles métier, codes ANSTAT)
  [ Silver Iceberg ]  ←── données propres, typées, enrichies
        │
        ▼ agrégation Spark (indicateurs, statistiques)
  [ Gold Iceberg ]    ←── prêt pour dashboards et exports CSV

        ↕ requêtes SQL ad hoc à tout moment
  [ Trino ]           ←── moteur SQL léger, sans lancer Spark
```

---

## Pourquoi ces technologies ?

Chaque brique technologique apporte une valeur précise à la plateforme.

### Apache Iceberg — le format de table

Sans Iceberg, les données Parquet sur un stockage objet (MinIO/S3) sont des fichiers inertes :
aucune transaction, aucun historique, aucune évolution de schéma sans réécriture complète.

**Ce qu'Iceberg apporte :**

| Besoin | Solution Iceberg |
|--------|-----------------|
| Écrire sans corrompre une table en cours de lecture | Transactions ACID — commit atomique |
| Relire les données telles qu'elles étaient hier ou le mois dernier | **Time travel** — `VERSION AS OF` ou `TIMESTAMP AS OF` |
| Ajouter une colonne sans rejouer tout le pipeline | **Schema evolution** — l'ancienne structure reste accessible |
| Ingérer de nouveaux mois sans réécrire les anciens | **Append** transactionnel, sans risque de doublon |
| Optimiser les lectures sur de grandes tables | Partitionnement caché, pruning automatique, metadata scanning |

> En pratique : les tables Bronze, Silver et Gold sont toutes des tables Iceberg. Un `spark_write_table(mode = "append")` en R ou un `df.writeTo(...).append()` en Python est une opération sûre, atomique, et annulable.

---

### Project Nessie — le versioning Git des données

Nessie est le catalogue qui référence toutes les tables Iceberg. Il fonctionne **comme Git** : chaque table a un historique de commits, et on peut créer des branches pour tester des transformations sans affecter la production.

**Ce qu'il apporte :**

- **Audit trail** complet : qui a modifié quoi, quand, avec quelle version de code
- **Rollback instantané** : restaurer une table à n'importe quel commit précédent
- **Branches de données** : tester une nouvelle règle métier sur une branche `dev`, fusionner sur `main` quand validé
- **Catalogue unifié** : Spark, Trino et les notebooks partagent le même référentiel de tables

> En pratique : la configuration `spark.sql.catalog.nessie.ref = "main"` dans les scripts R/Python pointe vers la branche principale. Changer ce paramètre en `"dev"` isole complètement les modifications.

---

### Apache Spark — le moteur de traitement

Spark est utilisé pour toutes les transformations lourdes : ingestion, Bronze→Silver, Silver→Gold.

**Ce qu'il apporte :**

- **Scalabilité transparente** : le même code R ou Python s'exécute sur un seul poste (mode `local`) et sur un cluster de 50 machines (mode `k8s://...`) — seul le `master` change
- **Lecture native Iceberg** : `spark_read_table()` ou `spark.table()` lit directement une table Iceberg versionnée sans copier les données
- **Traitement distribué des données volumineuses** : 31 millions de lignes × 10 ans traitées en quelques minutes
- **API multiple** : DataFrame API, SQL, et dplyr (via sparklyr en R) — chaque statisticien utilise ce qu'il connaît
- **Intégration Nessie/MinIO** : via les extensions Iceberg et le connecteur S3A

---

### Kubernetes + Spark Operator — la production sans serveur

Kubernetes (K8s) est le système d'orchestration du cluster. Le **Spark Operator** permet de soumettre des jobs Spark comme des ressources Kubernetes natives (fichiers YAML).

**Ce qu'ils apportent :**

| Sans K8s | Avec K8s + Spark Operator |
|----------|--------------------------|
| Spark tourne sur une machine dédiée, toujours allumée | Les ressources sont allouées uniquement pendant l'exécution du job |
| Déploiement manuel, dépendances à gérer sur chaque machine | Le job embarque ses dépendances dans un conteneur Docker reproductible |
| Pas d'isolation entre les jobs | Chaque job a son propre namespace, ses propres ressources |
| Redémarrage manuel en cas d'échec | Politique de retry configurable (`restartPolicy`) |
| Credentials exposés dans les scripts | Secrets Kubernetes (`secretKeyRef`) — jamais en clair dans le code |

> En pratique : un script R ou Python est d'abord développé en local avec `spark_connect(master = "local")`, puis mis en production via un fichier `SparkApplication.yaml` soumis à K8s avec `kubectl apply`. Voir [`Passage_du_Brouillon_à_k8s.md`](Passage_du_Brouillon_à_k8s.md).

---

### Trino — le SQL sans Spark

Trino est un moteur SQL distribué et léger, connecté au catalogue Nessie/Iceberg. Il permet d'interroger n'importe quelle table Bronze/Silver/Gold **sans démarrer Spark**.

**Ce qu'il apporte :**

- **Requêtes SQL interactives** en quelques secondes, là où Spark prend 30–60 s à démarrer
- **Consommation minimale** : aucun driver Spark, aucune JVM à instancier côté client
- **Compatibilité ODBC** : Stata peut se connecter directement via le driver ODBC Trino
- **Explorabilité** : idéal pour vérifier une table, valider un résultat, construire une requête ad hoc

> En pratique : dans les scripts d'exploration (`exploration.R`, `exploration.do`), l'option Trino est proposée en alternative à Spark pour les lectures simples.

---

### MinIO — le stockage objet on-premise

MinIO est un serveur de stockage compatible avec l'API Amazon S3, hébergé sur l'infrastructure interne.

**Ce qu'il apporte :**

- **Indépendance du cloud** : les données ne quittent jamais l'infrastructure ANSTAT
- **API S3 universelle** : tous les outils (Spark, R `aws.s3`, Python `boto3`/`s3fs`, Stata via Python) utilisent la même interface
- **Organisation par buckets** : `staging` (dépôt), `bronze`, `silver`, `gold` (couches Iceberg)
- **Accès multi-protocole** : HTTP depuis les postes locaux, DNS interne depuis le cluster K8s

---

## Endpoints

| Depuis | MinIO | Nessie (Spark) |
|--------|-------|----------------|
| Machine locale | `http://192.168.1.230:30137` | `http://192.168.1.230:30604/api/v1` |
| JupyterHub (cluster) | `http://minio.mon-namespace.svc.cluster.local:80` | `http://nessie.trino.svc.cluster.local:19120/api/v1` |

---

## Documentation

- [`docs/guide_utilisateur.md`](docs/guide_utilisateur.md) — **Guide complet** (architecture, workflows, langages, production)
- [`Passage_du_Brouillon_à_k8s.md`](Passage_du_Brouillon_à_k8s.md) — Passer un script en production (Spark Operator)
- [`templates/README.md`](templates/README.md) — Snippets d'import/export MinIO réutilisables
