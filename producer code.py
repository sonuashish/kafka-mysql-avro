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
    'bootstrap.servers' : 'pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'sasl.mechanisms' : 'PLAIN',
    'security.protocol' : 'SASL_SSL',
    'sasl.username' : 'MBRQ2XMEBJ43D3XJ',
    'sasl.password' : 'cfltxZ+ArECbcY1SB3x7d0Hj3wwL7kdlWr6iR9l8RYOxaZ8be0uqFehnLzZ3El0A'
}

schema_registry_client = SchemaRegistryClient({
    'url' : 'https://psrc-1yg3xnn.eastus.azure.confluent.cloud',
    'basic.auth.user.info' : '{}:{}'.format('MQPR4CPJG7E4OWEK','cflt0gfSmT0EV73LJdRSGdL7oh7/ezCyD+/5naW1kbBthSdnSbsRnbymdZUCUHlw')
})

# Fetch the latest  AVRO Schema for the Value
subject_name = 'Data1-value'
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
    'queue.buffering.max.messages': 200000,
    'queue.buffering.max.kbytes': 2097152  # 2 GB
})

# MySQL Connection Configuration
mysql_config = {
    'host' : 'localhost',
    'user' : 'root',
    'password' : '123456789',
    'database' : 'AZ'
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

topic = "Data1"

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

