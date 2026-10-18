import faust
import time

# Configuration de l'application Faust
app = faust.App(
    'kafka-streams-ventes',
    broker='kafka://localhost:9092',
    value_serializer='json',
    store='memory://',
    topic_partitions=1
)

# Définition du Topic
topic = app.topic(
    'ventes-stream', 
    value_serializer='json'
)

# Table en mémoire pour stocker le chiffre d'affaires par produit
ca_table = app.Table('ca_total_par_produit', default=float).tumbling(60.0, expires=120.0)

@app.agent(topic)
async def process_ventes(stream):
    async for vente in stream:
        try:
            produit = vente['produit']
            prix_total = vente['prix'] * vente['quantite']
            
            ca_table[produit] += prix_total
            
            # CORRECTION ICI : on utilise .now() pour obtenir la valeur actuelle de la fenêtre
            valeur_actuelle = ca_table[produit].now()
            
            print(f"[KAFKA STREAMS] {time.strftime('%H:%M:%S')} | Produit: {produit} | CA (fenêtre 1 min): {valeur_actuelle:.2f}€", flush=True)
            
        except Exception as e:
            print(f"Erreur de traitement : {e}")