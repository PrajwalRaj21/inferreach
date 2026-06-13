"""
producer.py
Generates fake Shopify-style orders and streams them to Kafka every second.
"""

import json
import time
import random
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / '.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC  = os.getenv('KAFKA_TOPIC', 'orders')

fake = Faker()

PRODUCTS = [
    {'id': 'P001', 'name': 'Wireless Headphones',  'price': 79.99,  'category': 'Electronics'},
    {'id': 'P002', 'name': 'Running Shoes',         'price': 129.99, 'category': 'Sports'},
    {'id': 'P003', 'name': 'Coffee Maker',          'price': 49.99,  'category': 'Kitchen'},
    {'id': 'P004', 'name': 'Yoga Mat',              'price': 29.99,  'category': 'Sports'},
    {'id': 'P005', 'name': 'Laptop Stand',          'price': 39.99,  'category': 'Electronics'},
    {'id': 'P006', 'name': 'Water Bottle',          'price': 19.99,  'category': 'Sports'},
    {'id': 'P007', 'name': 'Desk Lamp',             'price': 34.99,  'category': 'Home'},
    {'id': 'P008', 'name': 'Backpack',              'price': 59.99,  'category': 'Accessories'},
]

CHANNELS   = ['organic', 'paid_search', 'social', 'email', 'referral', 'direct']
COUNTRIES  = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'SG', 'IN']
STATUSES   = ['completed', 'completed', 'completed', 'pending', 'refunded']

def generate_order():
    product  = random.choice(PRODUCTS)
    qty      = random.randint(1, 4)
    discount = round(random.choice([0, 0, 0, 5, 10, 15]), 2)
    subtotal = round(product['price'] * qty, 2)
    total    = round(subtotal * (1 - discount/100), 2)

    return {
        'order_id':       fake.uuid4(),
        'customer_id':    fake.uuid4(),
        'customer_email': fake.email(),
        'product_id':     product['id'],
        'product_name':   product['name'],
        'category':       product['category'],
        'quantity':       qty,
        'unit_price':     product['price'],
        'discount_pct':   discount,
        'total_amount':   total,
        'currency':       'USD',
        'status':         random.choice(STATUSES),
        'channel':        random.choice(CHANNELS),
        'country':        random.choice(COUNTRIES),
        'created_at':     datetime.now(timezone.utc).isoformat(),
    }

def run():
    log.info(f'Connecting to Kafka at {KAFKA_BROKER}...')
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    log.info(f'Connected. Streaming orders to topic "{KAFKA_TOPIC}"...')

    count = 0
    while True:
        order = generate_order()
        producer.send(KAFKA_TOPIC, value=order)
        count += 1
        log.info(f'[{count}] Order {order["order_id"][:8]}... | {order["product_name"]} x{order["quantity"]} | ${order["total_amount"]} | {order["status"]}')
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == '__main__':
    run()