import pandas as pd
from confluent_kafka import Producer
import json
import time
import os

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

# 3. Préparation des données
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, '..', 'lakehouse', 'bronze', 'sample_sante.csv')
df = pd.read_csv(csv_path, sep=';', encoding='latin-1') 

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