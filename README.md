# Projet Big Data : Santé et Territoires 🏥📍
**Défi Open Data University**

Ce projet vise à construire un pipeline de données complet pour analyser les problématiques de santé à l'échelle territoriale, en utilisant une architecture Data Lake moderne.

## 🚀 Architecture Technique
Le projet repose sur une architecture **Medallion** avec les technologies suivantes :
* **Ingestion :** Kafka (Broker) & Python (Producers/Consumers)
* **Stockage Objet :** MinIO (S3 Compatible)
* **Traitement :** PySpark / Pandas
* **Orchestration :** Airflow (Phase finale)


## 📂 Structure du Data Lake (MinIO)
* `bronze/` : Données brutes ingérées via Kafka (format JSON/CSV).
* `silver/` : Données nettoyées, typées et dédoublonnées (format Parquet).
* `gold/` : Indicateurs agrégés et résultats de Machine Learning (format Parquet).


## 🛠️ Installation et Lancement

### 1. Pré-requis

- **Python 3.9+** 
- **Git** : Pour cloner le repository
- **Docker & Docker Compose**

### 2. Démarrage de l'infrastructure
```bash
docker-compose up -d
```
### 3. Configuration de l'envirennement Python
```bash
python -m venv venv
```
```bash
source venv/Scripts/activate
```
> ℹ️ Vous devriez voir `(venv)` au début de votre prompt après activation

```bash
pip install -r requirements.txt
```

#### 📦 Dépendances

- **confluent-kafka** : Client Kafka pour streaming de données
- **pandas** : Manipulation et analyse de données
- **boto3** : SDK AWS pour MinIO (stockage S3)

---

## 🤝 Convention de Commits
Pour assurer la lisibilité de l'historique du projet, nous utilisons le format suivant pour nos messages de commit :
`[Type] : Description courte`

Types utilisés :
- `[feat]` : Nouvelle fonctionnalité (ex: scripts, nouvelle étape)
- `[fix]` : Correction de bug (ex: encodage, erreur de chemin)
- `[docs]` : Mise à jour de la documentation (README, commentaires)
- `[infra]` : Configuration de l'environnement (Docker, Kafka, MinIO)

## 📝 État d'avancement (Mise à jour : 09/03/2026)

- [x] **Initialisation du Git et structure des dossiers**
- [x] **Infrastructure (Docker)** : Kafka et MinIO opérationnels
- [x] **Pipeline d'ingestion (Bronze)** :
    - [x] Script `producer.py` : Lecture du dataset `annuaire_sante.csv` et ingestion dans le topic `open-data-sante`.
    - [x] Script `consumer.py` : Persistance des messages JSON dans `datalake/bronze/date=YYYY-MM-DD/`.
- [ ] **Nettoyage (Silver)** : En cours...
- [ ] **Analyse et ML (Gold)** : À venir...

## 👥 Auteurs

Équipe projet Big Data - Santé et Territoires (3iL)
