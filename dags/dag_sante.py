from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 1. AJOUT DU CHEMIN POUR LE SCHEDULER (À mettre AVANT les imports de scripts)
sys.path.append('/root/.local/lib/python3.8/site-packages')
sys.path.append('/opt/airflow')

# Maintenant, les imports fonctionneront
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
    schedule_interval=None,
    catchup=False
) as dag:

    task_producer = PythonOperator(
        task_id='ingestion_kafka_producer',
        python_callable=run_producer
    )

    task_consumer = PythonOperator(
        task_id='stockage_minio_bronze',
        python_callable=run_consumer
    )

    task_silver = PythonOperator(
        task_id='transformation_silver',
        python_callable=run_cleaning
    )

    task_gold = PythonOperator(
        task_id='analyse_ml_gold',
        python_callable=run_ml_diagnostic
    )

    task_producer >> task_consumer >> task_silver >> task_gold