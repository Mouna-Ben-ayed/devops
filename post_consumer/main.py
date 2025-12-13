from kafka import KafkaConsumer
from google.cloud import bigquery
from pymongo import MongoClient
import json
import os

# Kafka
# Ces variables sont lues depuis les arguments de la commande ou l'environnement (non montré ici)
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
# Définir explicitement l'emplacement du fichier Secret monté par Kubernetes
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/secrets/service-account.json"
bq_client = bigquery.Client()
dataset_id = os.getenv("BQ_DATASET", "kafka_dataset")
table_id = os.getenv("BQ_TABLE", "posts_table")

def get_expected_schema():
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("post_type_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parent_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("accepted_answer_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("creation_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("score", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("view_count", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("owner_user_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_editor_user_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_edit_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_activity_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tags", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("answer_count", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("comment_count", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content_license", "STRING", mode="NULLABLE"),
    ]

def ensure_table_exists():
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    expected_schema = get_expected_schema()
    
    try:
        table = bq_client.get_table(table_ref)
        print(f"Table {dataset_id}.{table_id} exists")
        
        existing_fields = {field.name for field in table.schema}
        expected_fields = {field.name for field in expected_schema}
        missing_fields = expected_fields - existing_fields
        
        if missing_fields:
            print(f"Missing columns detected: {missing_fields}")
            new_schema = list(table.schema)
            for field in expected_schema:
                if field.name in missing_fields:
                    new_schema.append(field)
                    print(f"Adding column: {field.name}")
            
            table.schema = new_schema
            bq_client.update_table(table, ["schema"])
            print(f"Table schema updated with new columns")
        
        return bq_client.get_table(table_ref)
        
    except Exception:
        print(f"Table {dataset_id}.{table_id} not found, creating...")
        table = bigquery.Table(table_ref, schema=expected_schema)
        bq_client.create_table(table)
        print(f"Table {dataset_id}.{table_id} created")
        return bq_client.get_table(table_ref)

table_ref = ensure_table_exists()
# MongoDB Configuration
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb-service")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_DB = os.getenv("MONGO_DB", "posts_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "posts")

mongo_client = MongoClient(
    host=MONGO_HOST,
    port=MONGO_PORT,
    username=MONGO_USER,
    password=MONGO_PASSWORD
)
mongo_db = mongo_client[MONGO_DB]
mongo_collection = mongo_db[MONGO_COLLECTION]


print("Connected to MongoDB at {}:{}".format(MONGO_HOST, MONGO_PORT))
print(f"Starting Kafka consumer for topic {KAFKA_TOPIC}...")

for message in consumer:
    data = message.value
    print(f"Inserting: {data}")

    try: 
        errors = bq_client.insert_rows_json(table_ref, [data])
        if errors:
            print(f"BigQuery insertion errors: {errors}")
    except Exception as e:
        print(f"Error inserting into BigQuery: {e}")
        
    try:
        mongo_collection.insert_one(data.copy())
        print("Inserted into MongoDB")
    except Exception as e:
        print(f"Error inserting into MongoDB: {e}")
    print(f"Inserted into MongoDB with id: {data['id']}")