**Kafka Producer & Consumer Group with MySQL, Avro, and JSON Transformation**

**Overview**

This project demonstrates an end-to-end pipeline using **MySQL**, **Kafka**, **Confluent Schema Registry, **Avro serialization**, and **Python** to:

* Incrementally fetch data from a MySQL table.
* Serialize and publish messages to a Kafka topic.
* Consume messages as a consumer group.
* Transform the data and write results to **separate JSON files**.

---

## **Features**

* **Incremental MySQL data fetching** using an auto-increment ID.
* **Avro schema-based serialization** for Kafka producer and deserialization in consumers.
* **Kafka consumer group** with multiple threaded consumers for parallel processing.
* **Business logic transformation**: Category to uppercase and discount application.
* **JSON file writing**: Each consumer appends transformed records to a separate file, one JSON object per line.
* **Configurable and extensible Python scripts** with detailed logging.

---

## **Project Structure**

```
.
├── producer code.py           # Producer code to fetch from MySQL and push to Kafka
├── consumer code.py           # Consumer group to transform and write JSON files
├── product_schema.avsc        # Avro schema definition
├── queries.sql                # MySQL queries used for incremental fetch
├── random mysql data gen..py  # Python Code to generate Data to MySQL Table
├── json_output/               # Folder to store JSON output from each consumer
│   ├── consumer_1_data.json
│   ├── consumer_2_data.json
│   └── ...
└── README.md                  # Documentation
```

---

## **Pre-Requisites**

1. **Software:**

   * Python 3.9+
   * MySQL server
   * Apache Kafka / Confluent Cloud (cluster set up)
   * Confluent Schema Registry

2. **Python Libraries:**

   ```bash
   pip install confluent-kafka mysql-connector-python
   ```

3. **Kafka Topic & Schema Registry Setup:**

   * Create a Kafka topic: `product_updates`
    # Here I've used 'Data1' as topic name
   * Register an Avro schema for the topic in the Schema Registry.

---

## **Avro Schema**

Save the following content as `product_schema.avsc`:

```json
{
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "category",
      "type": "string"
    },
    {
      "default": null,
      "name": "price",
      "type": [
        "null",
        "float"
      ]
    },
    {
      "name": "last_update",
      "type": {
        "logicalType": "timestamp-millis",
        "type": "long"
      }
    }
  ],
  "name": "Purchase",
  "namespace": "local_data",
  "type": "record"
}
```

---

## **SQL Queries for Incremental Fetch**

Example query to fetch new data based on the last processed ID:

```sql
SELECT *  
FROM product 
WHERE id > %s 
ORDER BY id ASC;
```


* Maintain the last processed ID in a state file or variable.
* Replace `%s` with the last fetched ID.

---

## **Running the Producer**

1. Update `producer.py` with:

   * MySQL credentials.
   * Kafka cluster details.
   * Schema Registry configuration.
2. Run the producer:

   ```bash
   python producer.py
   ```
3. The producer will:

   * Connect to MySQL.
   * Fetch incremental records.
   * Serialize using Avro.
   * Produce to the Kafka topic `product_updates` with partition key (category).

---

## **Running the Consumer Group**

1. Update `consumer_group.py` with Kafka and Schema Registry details.
2. Run the consumer group:

   ```bash
   python consumer_group.py
   ```
3. The consumer group will:

   * Subscribe to `product_updates`.
   * Transform each message (uppercase category, apply discount).
   * Write each transformed record to a **consumer-specific JSON file** in `json_output/`.
   * Each line is one JSON object for easy readability.

---

## **Transformation Logic**

The consumer applies:

1. **Category uppercase**: `record['category'] = record.get('category', '').upper()`
2. **Discount rules**:

   ```python
   DISCOUNT_RULES = {
       'ELECTRONICS': Decimal('0.10'),
       'FASHION': Decimal('0.15')
   }
   price = Decimal(str(record.get('price', 0)))
   discount = DISCOUNT_RULES.get(record['category'], Decimal('0'))
   record['price'] = float(price * (1 - discount))
   ```

---

## **Outputs**

* Each consumer writes to a file:

  ```
  json_output/consumer_1_data.json
  json_output/consumer_2_data.json
  ```
* Example JSON line:

  ```json
  {"id": 1, "name": "Smartphone", "category": "ELECTRONICS", "price": 450.0, "created_at": "2025-08-20"}
  ```
---