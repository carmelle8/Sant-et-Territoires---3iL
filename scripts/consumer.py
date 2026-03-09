from confluent_kafka import Consumer
import boto3
import json
from datetime import datetime

# 1. Configuration Kafka
kafka_conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "sante-consumer-group",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_conf)
consumer.subscribe(['open-data-sante'])

# 2. Configuration MinIO (S3)
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)
bucket_name = "datalakesante"

print("En attente de messages...")

try:
    while True:
        msg = consumer.poll(1.0) # Attente d'un message pendant 1s
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur : {msg.error()}")
            continue

        # Récupération de la donnée
        data = json.loads(msg.value().decode('utf-8'))
        
        # Définition du chemin dans MinIO (Partitionnement par date)
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H-%M-%S-%f")
        file_path = f"bronze/date={date_str}/data_{timestamp}.json"

        # Envoi vers MinIO
        s3.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=json.dumps(data)
        )
        print(f"Fichier sauvegardé : {file_path}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()