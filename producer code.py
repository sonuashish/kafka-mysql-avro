import time
import mysql.connector
from datetime import datetime
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer

#Delivery Callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for product {msg.key()}: {err}")
    else:
        print(f"Product {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# kafka and Schema registry configuration
kafka_config =  {
    'bootstrap.servers' : 'url',
    'sasl.mechanisms' : 'PLAIN',
    'security.protocol' : 'SASL_SSL',
    'sasl.username' : 'API key',
    'sasl.password' : 'API Passowrd'
}

schema_registry_client = SchemaRegistryClient({
    'url' : 'url',
    'basic.auth.user.info' : '{}:{}'.format('','')
})

# Fetch the latest  AVRO Schema for the Value
subject_name = 'product_updates-value'
schma_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

#Producer Setup
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schma_str)

producer = SerializingProducer({
    'bootstrap.servers' : kafka_config['bootstrap.servers'],
    'security.protocol' : kafka_config['security.protocol'],
    'sasl.mechanisms' : kafka_config['sasl.mechanisms'],
    'sasl.username' : kafka_config['sasl.username'],
    'sasl.password' : kafka_config['sasl.password'],
    'key.serializer' : key_serializer,
    'value.serializer' : avro_serializer,
    'queue.buffering.max.messages': 200000, # -------- Added these two lines as my Producer was crashing while producing 
    'queue.buffering.max.kbytes': 2097152  # 2 GB  --- and consuming at the same time for millions of records.
})

# MySQL Connection Configuration
mysql_config = {
    'host' : 'localhost',
    'user' : '',
    'password' : '',
    'database' : 'Z'
}

# To Fetch new Records
def fetch_new_records(last_timestamp):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)
    # SQL QUERY TO FETCH INCREMENTAL DATA FROM MYSQL
    query = "SELECT * FROM product WHERE last_update > %s ORDER BY last_update ASC"
    cursor.execute(query, (last_timestamp,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

# Initialize last read timestamp
last_read_timestamp = datetime.min.strftime('%Y-%m-%d %H:%M:%S')

topic = "product_updates"

# Continuous Fetch and Produce Loop
while True:
    records = fetch_new_records(last_read_timestamp)
    if records:
        for record in records:
            key = str(record['id'])
            # Successful Writing of Data to Kafka Topic with Proper Partitioning
            producer.produce(topic=topic, key=key, value=record, on_delivery=delivery_report)
            producer.poll(0)
            last_read_timestamp = record['last_update']
        producer.flush()
    time.sleep(10)

