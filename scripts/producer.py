import sys
import os
sys.path.append('/root/.local/lib/python3.8/site-packages')

import pandas as pd
import json
from kafka import KafkaProducer

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],  # port interne Docker
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
    )

    # Les CSV sont dans /opt/airflow/data/bronze/ (= ton dossier data/bronze/ sur le PC)
    DATA_DIR = "/opt/airflow/data/bronze"

    files = [
        'communes-france-2025.csv',
        'correspondance-tvs-communes-2018.csv',
        'etalab-cs1100502-stock-20260107-0343.csv',
        'etalab-cs1100507-stock-20260107-0342.csv',
        'patientele.csv',
        'vf-base-sante-garches.csv'
    ]

    print("📡 Démarrage de l'ingestion vers Kafka...")

    for file_name in files:
        path = os.path.join(DATA_DIR, file_name)

        if os.path.exists(path):
            print(f"📖 Lecture de : {file_name}")
            df = pd.read_csv(path, low_memory=False)
            print(f"   → {len(df)} lignes trouvées")

            for _, row in df.iterrows():
                message = {"source": file_name, "data": row.to_dict()}
                producer.send('topic-sante-brut', value=message)

            print(f"   ✅ {file_name} envoyé vers Kafka")
        else:
            print(f"⚠️  Fichier introuvable : {path}")

    producer.flush()
    print("✅ Ingestion Kafka terminée.")

if __name__ == "__main__":
    run_producer()