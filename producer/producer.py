import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

produits = ["Laptop", "Smartphone", "Casque", "Écran", "Clavier", "Souris"]

print("Démarrage de la simulation du flux de ventes (100 événements/seconde)...")

try:
    while True:
        for _ in range(100): # Génère 100 événements par seconde pour tester le débit
            vente = {
                "transaction_id": random.randint(10000, 99999),
                "produit": random.choice(produits),
                "prix": round(random.uniform(20.0, 1200.0), 2),
                "quantite": random.randint(1, 5),
                "timestamp": int(time.time() * 1000) # Timestamp pour mesurer la latence
            }
            producer.send('ventes-stream', value=vente)
        
        producer.flush()
        time.sleep(1) # Pause d'une seconde avant le prochain lot
except KeyboardInterrupt:
    print("Arrêt du producteur.")
    producer.close()