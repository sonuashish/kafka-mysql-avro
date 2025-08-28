import threading
import logging
import json
from datetime import datetime
from decimal import Decimal
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')

# Kafka configuration - matches the producer cluster
def get_kafka_config():
    return {
        'bootstrap.servers': 'url',
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'key',
        'sasl.password': 'passord',
        'group.id': 'product_updates_group',
        'auto.offset.reset': 'earliest'
    }

# Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': '',
    'basic.auth.user.info': '{}:{}'.format('','')
})

subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Deserializers
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Business rules for discounting
DISCOUNT_RULES = {
    'ELECTRONICS': Decimal('0.10'),
    'FASHION': Decimal('0.15')
}

# DATA Transformation logic
def transform_record(record):
    if record is None:
        return None
    # Category to uppercase
    record['category'] = record.get('category', '').upper()
    # Apply discount if category matches
    price = Decimal(str(record.get('price', 0)))
    discount = DISCOUNT_RULES.get(record['category'], Decimal('0'))
    record['price'] = float(price * (1 - discount))
    return record

# Custom serializer to handle Decimal and datetime for JSON
def custom_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def consume(worker_id):
    consumer = DeserializingConsumer({
        **get_kafka_config(),
        'key.deserializer': key_deserializer,
        'value.deserializer': avro_deserializer
    })
    consumer.subscribe(['product_updates'])

    logging.info(f"Consumer {worker_id} started.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            record = transform_record(msg.value())
            logging.info(f"Worker {worker_id}: key={msg.key()} value={record}")

            # Convert to JSON and append to file
            if record:
                json_string = json.dumps(record, default=custom_serializer)
                with open(f"consumer_{worker_id}_data.json", "a") as f:
                    f.write(json_string + "\n")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logging.info(f"Consumer {worker_id} closed.")

# Launch 5 consumers
threads = []
for i in range(5):
    t = threading.Thread(target=consume, args=(i+1,), name=f"Consumer-{i+1}")
    t.start()
    threads.append(t)

for t in threads:
    t.join()