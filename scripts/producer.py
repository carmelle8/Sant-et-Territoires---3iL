import pandas as pd
from confluent_kafka import Producer
import json
import time
import os
import boto3
import json

# 1. Configuration et Initialisation
conf = {'bootstrap.servers': "localhost:9092"} # On dit au script où se trouve le serveur Kafka
producer = Producer(conf)

# 2. La fonction de Callback (Accusé de réception)
def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur d'envoi : {err}")
    else:
        # Si ça marche, on affiche le nom du topic et le numéro de partition
        print(f"Message envoyé à {msg.topic()}")

# 3. Configuration de la connexion MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

bucket_name = 'datalakesante'
prefix = 'bronze/date=2026-03-05/'

# Lister tous les fichiers CSV dans le dossier
print(f"Lecture des fichiers CSV depuis MinIO/{bucket_name}/{prefix}...")

# Debug: lister tous les buckets
try:
    buckets = s3.list_buckets()
    print(f"\nBuckets disponibles : {[b['Name'] for b in buckets['Buckets']]}")
except Exception as e:
    print(f"Erreur lors de la connexion à MinIO : {e}")
    exit(1)

# Debug: lister tous les objets avec le prefix
try:
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    print(f"Objets trouvés avec prefix '{prefix}' : {response.get('KeyCount', 0)}")
    if 'Contents' in response:
        print("Fichiers trouvés :")
        for obj in response['Contents']:
            print(f"  - {obj['Key']}")
    else:
        print("Aucun fichier trouvé. Essai sans prefix...")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print("Tous les fichiers du bucket :")
            for obj in response['Contents']:
                print(f"  - {obj['Key']}")
except Exception as e:
    print(f"Erreur lors de la lecture du bucket : {e}")
    exit(1)

response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Récupérer et combiner tous les fichiers CSV
dfs = []
if 'Contents' in response:
    for obj in response['Contents']:
        if obj['Key'].endswith('.csv'):
            print(f"Traitement du fichier : {obj['Key']}")
            try:
                # Lire le fichier CSV depuis MinIO
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                # Essayer avec ';' d'abord, puis avec ','
                try:
                    df_temp = pd.read_csv(file_obj['Body'], sep=';', encoding='latin-1', on_bad_lines='skip')
                except:
                    file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                    df_temp = pd.read_csv(file_obj['Body'], sep=',', encoding='utf-8', on_bad_lines='skip')
                
                print(f"  ✓ Colonnes : {list(df_temp.columns)}")
                print(f"  ✓ Lignes : {len(df_temp)}")
                dfs.append(df_temp)
            except Exception as e:
                print(f"  ✗ Erreur lors de la lecture : {e}")
                continue

# Combiner tous les DataFrames (avec toutes les colonnes)
if dfs:
    # join='outer' garde toutes les colonnes et remplit les manquantes avec NaN
    df = pd.concat(dfs, ignore_index=True, join='outer', sort=False)
    print(f"\nTotal de lignes : {len(df)}")
    print(f"Total de colonnes : {len(df.columns)}")
    print(f"Colonnes finales : {list(df.columns)}")
else:
    print("Aucun fichier CSV trouvé dans MinIO")
    exit(1) 

print("Début de l'envoi...")

# 4. La boucle d'envoi
for index, row in df.iterrows():
    # Kafka ne comprend pas les lignes Excel/Pandas. 
    # On transforme la ligne en DICTIONNAIRE Python, puis en chaîne JSON (texte).
    data = row.to_dict()
    payload = json.dumps(data)
    
    # On envoie le message vers le topic 'open-data-sante'
    producer.produce('open-data-sante', value=payload, callback=delivery_report)
    
    # 5. Gestion du trafic
    producer.poll(0) # Sert à déclencher les rapports de livraison (callback)
    time.sleep(0.1)  # On ralentit un peu pour ne pas saturer Kafka et simuler du "temps réel"

# 6. Nettoyage final
producer.flush() # On force l'envoi des derniers messages restés dans le buffer
print("Fin de l'ingestion.")