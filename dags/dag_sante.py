import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ✅ CORRECTION : on ajoute /opt/airflow au path de façon sécurisée
# Cela permet à Airflow de trouver les scripts peu importe le contexte
AIRFLOW_HOME = '/opt/airflow'
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

# Import des fonctions depuis les scripts
from scripts.producer import run_producer
from scripts.consumer import run_consumer
from scripts.silver_processor import run_cleaning
from scripts.gold_analytics import run_ml_diagnostic

default_args = {
    'owner': 'Equipe_3iL',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_sante_territoire_final',
    default_args=default_args,
    description='Pipeline Medallion : Kafka -> MinIO (Bronze/Silver/Gold)',
    schedule_interval=None,  # ✅ manuel : on clique sur Trigger DAG
    catchup=False
) as dag:

    # ÉTAPE 1 : Lire les CSV et envoyer vers Kafka
    task_producer = PythonOperator(
        task_id='ingestion_kafka_producer',
        python_callable=run_producer
    )

    # ÉTAPE 2 : Lire Kafka et stocker dans MinIO Bronze
    task_consumer = PythonOperator(
        task_id='stockage_minio_bronze',
        python_callable=run_consumer
    )

    # ÉTAPE 3 : Nettoyer les données → couche Silver
    task_silver = PythonOperator(
        task_id='transformation_silver',
        python_callable=run_cleaning
    )

    # ÉTAPE 4 : Machine Learning → couche Gold
    task_gold = PythonOperator(
        task_id='analyse_ml_gold',
        python_callable=run_ml_diagnostic
    )

    # ✅ Ordre d'exécution automatique
    task_producer >> task_consumer >> task_silver >> task_gold
