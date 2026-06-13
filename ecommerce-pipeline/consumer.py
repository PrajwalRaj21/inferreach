"""
consumer.py
Reads orders from Kafka and loads them into BigQuery raw table.
"""

import json
import logging
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC  = os.getenv('KAFKA_TOPIC', 'orders')
PROJECT_ID   = os.getenv('GCP_PROJECT_ID')
DATASET      = os.getenv('BQ_DATASET', 'ecommerce')
TABLE        = 'raw_orders'
KEY_PATH     = os.getenv('GCP_KEY_PATH', 'gcp-key.json')

# BigQuery schema
SCHEMA = [
    bigquery.SchemaField('order_id',       'STRING'),
    bigquery.SchemaField('customer_id',    'STRING'),
    bigquery.SchemaField('customer_email', 'STRING'),
    bigquery.SchemaField('product_id',     'STRING'),
    bigquery.SchemaField('product_name',   'STRING'),
    bigquery.SchemaField('category',       'STRING'),
    bigquery.SchemaField('quantity',       'INTEGER'),
    bigquery.SchemaField('unit_price',     'FLOAT'),
    bigquery.SchemaField('discount_pct',   'FLOAT'),
    bigquery.SchemaField('total_amount',   'FLOAT'),
    bigquery.SchemaField('currency',       'STRING'),
    bigquery.SchemaField('status',         'STRING'),
    bigquery.SchemaField('channel',        'STRING'),
    bigquery.SchemaField('country',        'STRING'),
    bigquery.SchemaField('created_at',     'TIMESTAMP'),
    bigquery.SchemaField('ingested_at',    'TIMESTAMP'),
]

def get_bq_client():
    creds = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(project=PROJECT_ID, credentials=creds)

def ensure_table(client):
    dataset_ref = client.dataset(DATASET)
    # Create dataset if not exists
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        client.create_dataset(dataset)
        log.info(f'Created dataset {DATASET}')

    # Create table if not exists
    table_ref = dataset_ref.table(TABLE)
    try:
        client.get_table(table_ref)
        log.info(f'Table {DATASET}.{TABLE} already exists')
    except Exception:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = client.create_table(table)
        log.info(f'Created table {DATASET}.{TABLE}')

    return client.get_table(table_ref)

def run():
    if not PROJECT_ID:
        log.error('GCP_PROJECT_ID not set in .env')
        return

    log.info('Connecting to BigQuery...')
    bq = get_bq_client()
    table = ensure_table(bq)
    log.info(f'BigQuery ready: {PROJECT_ID}.{DATASET}.{TABLE}')

    log.info(f'Connecting to Kafka at {KAFKA_BROKER}...')
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='inferreach-consumer',
        consumer_timeout_ms=-1
    )
    log.info(f'Consuming from topic "{KAFKA_TOPIC}"...')

    buffer = []
    BATCH_SIZE = 10

    for message in consumer:
        order = message.value
        order['ingested_at'] = datetime.now(timezone.utc).isoformat()
        buffer.append(order)

        if len(buffer) >= BATCH_SIZE:
            errors = bq.insert_rows_json(table, buffer)
            if errors:
                log.error(f'BigQuery insert errors: {errors}')
            else:
                log.info(f'Inserted {len(buffer)} rows to BigQuery')
            buffer = []

if __name__ == '__main__':
    run()