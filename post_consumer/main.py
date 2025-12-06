import logging
import json
import os
import argparse
from kafka import KafkaConsumer
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TOPIC = "posts"
SCHEMA = [
    bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('post_type_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('accepted_answer_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('creation_date', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('score', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('view_count', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('body', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('owner_user_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('last_editor_user_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('last_edit_date', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('last_activity_date', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tags', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('answer_count', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('comment_count', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('content_license', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('parent_id', 'STRING', mode='NULLABLE')
]

def create_dataset_if_not_exists(client, dataset_id, project_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        log.info(f"Dataset {dataset_id} already exists.")
    except Exception:
        log.info(f"Dataset {dataset_id} does not exist. Creating it.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset.default_table_expiration_ms = 3600000 * 24
        client.create_dataset(dataset)
        log.info(f"Created dataset {dataset_id}.")

def save_post_to_json(post, filepath):
    with open(filepath, 'w') as json_file:
        json.dump(post, json_file)

def post_bigquery(client, table_ref, post):
    temp_filepath = '/tmp/post_consumer.json'
    save_post_to_json(post, temp_filepath)

    with open(temp_filepath, 'rb') as json_file:
        job = client.load_table_from_file(json_file, table_ref, job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        ))

    job.result()

    if job.errors is None:
        log.info(f"Inserted post with id {post.get('id')}")
    else:
        log.info("Encountered errors while inserting rows: {}".format(job.errors))

def main(kafka_host):
    # BigQuery Setup
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./service-account.json"
    client = bigquery.Client()
    project_id = client.project
    dataset_id = 'data_devops'
    table_id = 'posts'

    create_dataset_if_not_exists(client, dataset_id, project_id)

    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)
        log.info(f"Table {table_id} already exists.")
    except Exception:
        log.info(f"Table {table_id} does not exist. Creating it.")
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)
        log.info(f"Created table {table_id}.")

    # Kafka Consumer Setup
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[kafka_host],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='post-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    log.info(f"Listening to topic {TOPIC} on {kafka_host}...")

    for message in consumer:
        post = message.value
        log.info(f"Received post: {post.get('id')}")
        post_bigquery(client, table_ref, post)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_host', type=str, required=False, default='kafka-broker-service:9092', help='The Kafka host address')
    args = parser.parse_args()

    main(args.kafka_host)
