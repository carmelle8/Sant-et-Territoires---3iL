import sys
import os
sys.path.append('/root/.local/lib/python3.8/site-packages')

import pandas as pd
import json
from kafka import KafkaProducer

def detect_separator(path):
    """Détecte automatiquement le séparateur du fichier CSV (virgule ou point-virgule)"""
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
    if first_line.count(';') > first_line.count(','):
        return ';'
    return ','

def run_producer():
    # ✅ Port 29092 = port INTERNE Docker (entre containers)
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
    )

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

            # ✅ Détection automatique du séparateur
            sep = detect_separator(path)
            print(f"   → Séparateur détecté : '{sep}'")

            df = pd.read_csv(path, low_memory=False, sep=sep, on_bad_lines='skip')
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
