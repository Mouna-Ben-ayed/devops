from kafka import KafkaConsumer
from google.cloud import bigquery
import json
import os

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-broker-service:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "posts")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='consumer-group-python',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# BigQuery
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/service-account.json"
client = bigquery.Client()
dataset_id = os.getenv("BQ_DATASET", "kafka_dataset")
table_id = os.getenv("BQ_TABLE", "posts_table")

table_ref = client.dataset(dataset_id).table(table_id)

print(f"Starting Kafka consumer for topic {KAFKA_TOPIC}...")

for message in consumer:
    data = message.value
    print(f"Inserting: {data}")

    errors = client.insert_rows_json(table_ref, [data])
    if errors:
        print(f"Encountered errors: {errors}")
