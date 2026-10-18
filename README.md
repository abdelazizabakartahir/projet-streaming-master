# Projet Streaming Master - Sujet 08

## Prérequis
- Docker Desktop
- Python 3.8+
- Apache Spark (avec Hadoop winutils configuré)

## Installation
1. Lancer l'infrastructure : `cd docker && docker-compose up -d`
2. Installer les dépendances Python : `pip install -r requirements.txt` (kafka-python, pyspark, faust-streaming)
3. Lancer le producteur : `python producer/producer.py`
4. Lancer Spark : `python spark_consumer/spark_consumer.py`
5. Lancer Kafka Streams (Faust) : `faust -A kafka_streams_consumer/kafka_streams_app worker -l info`

## Résultats
- L'interface Kafka-UI est disponible sur http://localhost:8080
- Les benchmarks de latence sont visibles dans la console Spark.