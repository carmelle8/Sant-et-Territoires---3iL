# 🏥 Santé & Territoires — Pipeline Big Data
**Défi Open Data University · 3iL Limoges · Promotion 2025-2026**

> Construire un pipeline de données complet pour diagnostiquer les inégalités d'accès aux soins en France et identifier les déserts médicaux à l'échelle territoriale.

---

## 👥 Équipe

| Membre | Rôle principal |
|--------|---------------|
| Zélie Carmelle | Data Engineering & Orchestration |
| [Membre 2] | Machine Learning & Analyse |
| [Membre 3] | Visualisation & Documentation |

---

## 🎯 Objectifs du projet

Le défi **Santé & Territoires** vise à :

1. **Cartographier l'offre de soins** par département et type de territoire
2. **Identifier les profils territoriaux** (urbain, intermédiaire, rural) via du clustering
3. **Prédire la demande théorique** de soins pour détecter les zones sous-dotées
4. **Visualiser le diagnostic** à destination des collectivités locales

---

## 🏗️ Architecture technique

```
Sources Open Data (data.gouv.fr)
        │
        ▼
   [Producer Python]
        │  CSV → messages JSON
        ▼
   [Kafka Topic: topic-sante-brut]
        │  streaming
        ▼
   [Consumer Python]
        │  messages → fichiers CSV partitionnés par date
        ▼
   MinIO (Data Lake S3-compatible)
   ├── bronze/date=YYYY-MM-DD/   ← données brutes
   ├── silver/                   ← données nettoyées (Parquet)
   └── gold/                     ← résultats ML (Parquet)
        │
        ▼
   [Apache Airflow DAG]          ← orchestration du pipeline
        │
        ▼
   [Dashboard Streamlit]         ← visualisation interactive
```

### Stack technologique

| Couche | Technologie | Rôle |
|--------|-------------|------|
| Ingestion | **Kafka + Python** | Streaming des données brutes |
| Stockage | **MinIO (S3)** | Data Lake — 3 couches Medallion |
| Traitement | **Pandas + PyArrow** | Nettoyage et transformation |
| ML | **XGBoost + Scikit-Learn** | Prédiction et clustering |
| Orchestration | **Apache Airflow** | Planification du pipeline |
| Visualisation | **Streamlit + Plotly** | Dashboard interactif |
| Infrastructure | **Docker Compose** | Environnement reproductible |

---

## 📂 Structure du projet

```
Sant-et-Territoires---3iL/
│
├── docker-compose.yml          # Infrastructure complète
├── requirements.txt            # Dépendances Python
├── .gitignore                  # Exclut les données et l'environnement
├── README.md                   # Ce fichier
│
├── dags/
│   ├── __init__.py
│   └── dag_sante.py            # DAG Airflow — orchestration du pipeline
│
├── scripts/
│   ├── __init__.py
│   ├── producer.py             # Envoi des CSV vers Kafka
│   ├── consumer.py             # Kafka → MinIO Bronze
│   ├── silver_processor.py     # Bronze → Silver (nettoyage + jointures)
│   └── gold_analytics.py       # Silver → Gold (XGBoost + clustering)
│
├── data/
│   └── bronze/                 # Données brutes CSV (non versionnées)
│       ├── communes-france-2025.csv
│       ├── correspondance-tvs-communes-2018.csv
│       ├── etalab-cs1100502-stock-*.csv   (FINESS établissements)
│       ├── etalab-cs1100507-stock-*.csv   (FINESS équipements)
│       ├── patientele.csv
│       └── vf-base-sante-garches.csv
│
└── visualization/
    └── dashboard.py                # Application Streamlit
```

---

## 📊 Données utilisées

| Dataset | Source | Description | Couche |
|---------|--------|-------------|--------|
| Patientèle médecins | CNAM / data.gouv.fr | Nombre de patients par médecin et département | Bronze → Gold |
| FINESS établissements | DREES / data.gouv.fr | Répertoire national des établissements de santé | Bronze → Silver |
| Communes France 2025 | INSEE / data.gouv.fr | Population, densité, code INSEE par commune | Bronze → Silver |
| Correspondance TVS | CNAM | Secteurs de soins de proximité | Bronze → Silver |
| Base santé Garches | Open Data | Indicateurs santé environnementaux | Bronze → Silver |

---

## 🚀 Installation et lancement

### Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et démarré
- Python 3.9+
- Git

### 1. Cloner le dépôt

```bash
git clone https://github.com/VOTRE_REPO/Sant-et-Territoires---3iL.git
cd Sant-et-Territoires---3iL
```

### 2. Préparer les données

Télécharger les datasets depuis [data.gouv.fr](https://www.data.gouv.fr) et les placer dans `data/bronze/`.

Liens de téléchargement :
- [Patientèle CNAM](https://www.data.gouv.fr/fr/datasets/patientele-des-medecins-liberaux/)
- [FINESS](https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/)
- [Communes INSEE](https://www.data.gouv.fr/fr/datasets/communes-de-france/)

### 3. Démarrer l'infrastructure Docker

```bash
# Sur Windows (Git Bash)
export MSYS_NO_PATHCONV=1
docker-compose up -d
```

> ⏳ Le premier démarrage prend 3-5 minutes (installation des packages Python dans Airflow).

### 4. Vérifier que tout est UP

```bash
docker ps
```

Tous les containers doivent afficher `Up` :
- `zookeeper`, `kafka`, `minio`, `postgres`
- `airflow-init` → `Exited (0)` ✅ (normal, tâche unique)
- `airflow-webserver`, `airflow-scheduler`

### 5. Lancer le pipeline via Airflow

1. Ouvrir **http://localhost:8081** (admin / admin)
2. Activer le DAG `pipeline_sante_territoire_final` (interrupteur bleu)
3. Cliquer sur ▶️ **Trigger DAG**
4. Observer les tâches passer au vert une par une

### 6. Vérifier les données dans MinIO

Ouvrir **http://localhost:9001** (minioadmin / minioadmin)

Après un run complet, le bucket `datalakesante` contiendra :
```
bronze/date=YYYY-MM-DD/    ← 6 fichiers CSV ingérés
silver/                    ← final_health_territory.parquet
gold/                      ← final_diagnostic_results.parquet
```

### 7. Lancer le dashboard Streamlit

```bash
# Dans un terminal sur ton PC (hors Docker)
pip install streamlit plotly boto3 pandas pyarrow
streamlit run dashboard.py
```

Ouvrir **http://localhost:8501**

---

## 🔄 Pipeline de données (DAG Airflow)

```
ingestion_kafka_producer
        │ ✅ Envoie les 6 CSV vers le topic Kafka "topic-sante-brut"
        ▼
stockage_minio_bronze
        │ ✅ Lit Kafka et écrit les CSV dans MinIO bronze/date=YYYY-MM-DD/
        ▼
transformation_silver
        │ ✅ Nettoie, joint (FINESS × Patientèle), calcule les ratios
        │   Sortie : silver/final_health_territory.parquet
        ▼
analyse_ml_gold
          ✅ Clustering K-Means (3 profils) + XGBoost (R² ~0.93)
          Calcule l'écart offre/demande
          Sortie : gold/final_diagnostic_results.parquet
```

---

## 📈 Méthode d'analyse (Gold Layer)

### Clustering territorial (K-Means, K=3)

Segmentation des territoires en 3 profils homogènes :

| Cluster | Profil | Caractéristiques |
|---------|--------|-----------------|
| 0 | Urbain dense | Forte densité, nombreux établissements |
| 1 | Intermédiaire | Densité moyenne, accès partiel |
| 2 | Rural isolé | Faible densité, potentiel désert médical |

### Modèle prédictif (XGBoost Regressor)

- **Cible** : `patients_uniques` (volume de patients par territoire)
- **Features** : population, nb_établissements, nb_tvs, ratios par habitant, cluster
- **Performance** : R² ≈ 0.93 (validation croisée 5-fold)
- **Correction data leakage** : les ratios sont calculés sans utiliser la variable cible

### Indicateur de désert médical

```
Écart (%) = (Offre réelle − Offre théorique) / Offre théorique × 100
```

- Écart **négatif** 🔴 → territoire sous-doté (désert médical potentiel)
- Écart **positif** 🟢 → territoire correctement doté

---

## 🤝 Convention de commits

Format : `[type] : description courte`

| Type | Usage |
|------|-------|
| `[feat]` | Nouvelle fonctionnalité |
| `[fix]` | Correction de bug |
| `[docs]` | Documentation |
| `[infra]` | Docker, Kafka, MinIO |
| `[refactor]` | Restructuration du code |
| `[data]` | Modification des données ou scripts ML |

Exemples :
```bash
git commit -m "[feat] : ajout du clustering K-Means dans gold_analytics.py"
git commit -m "[fix] : correction port Kafka kafka:29092 dans producer.py"
git commit -m "[docs] : mise à jour README avec architecture finale"
```

---

## 📝 État d'avancement

- [x] Infrastructure Docker (Kafka, MinIO, Airflow, Postgres)
- [x] Pipeline Bronze : ingestion Kafka → MinIO
- [x] Pipeline Silver : nettoyage, jointures, feature engineering
- [x] Pipeline Gold : clustering K-Means + XGBoost
- [x] Orchestration Airflow (DAG complet)
- [ ] Dashboard Streamlit (en cours)
- [ ] Visualisation carte des déserts médicaux
- [ ] Préparation soutenance

---

## 📚 Références

- [Défi Santé & Territoires — Open Data University](https://defis.data.gouv.fr/defis/sante-et-territoires)
- [Diagnostic Territorial de Santé — Fondation Roche](https://www.fondationroche.org)
- [Documentation Apache Airflow](https://airflow.apache.org/docs/)
- [Documentation MinIO](https://min.io/docs/)
- [XGBoost Documentation](https://xgboost.readthedocs.io/)