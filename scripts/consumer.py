import sys
sys.path.append('/root/.local/lib/python3.8/site-packages')

from kafka import KafkaConsumer
import boto3
import json
from datetime import datetime

def run_consumer():
    # ✅ CORRECTION : port 29092 (port INTERNE Docker, entre containers)
    # ❌ AVANT : kafka:9092  → c'est le port EXTERNE (pour ton PC seulement)
    consumer = KafkaConsumer(
        'topic-sante-brut',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000  # ✅ augmenté à 30s pour les gros fichiers
    )

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    BUCKET = 'datalakesante'
    partition_date = datetime.now().strftime("%Y-%m-%d")

    # Créer le bucket s'il n'existe pas
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"✅ Bucket '{BUCKET}' créé.")

    # Regrouper les messages par fichier source
    data_bundles = {}

    print("📥 Écoute du topic Kafka...")
    for message in consumer:
        source = message.value['source']
        if source not in data_bundles:
            data_bundles[source] = []
        data_bundles[source].append(message.value['data'])

    if not data_bundles:
        print("⚠️  Aucun message reçu depuis Kafka.")
        return

    # Écrire chaque source dans MinIO (couche Bronze, partitionné par date)
    for source_name, records in data_bundles.items():
        s3_key = f"bronze/date={partition_date}/{source_name}"
        body = json.dumps(records, default=str)
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=body)
        print(f"✅ {len(records)} lignes → {s3_key}")

    print("🚀 Couche Bronze alimentée dans MinIO.")

if __name__ == "__main__":
    run_consumer()
