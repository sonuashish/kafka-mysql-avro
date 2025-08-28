import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import random

# MySQL connection details
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456789',  # change to your password
    'database': 'AZ'
}

# Categories and sample products
categories = {
    "Electronics": ["Mobile", "Laptop", "Tablet", "Headphones", "Camera"],
    "Fashion": ["Shirt", "Shoes", "Watch", "Bag", "Sunglasses"],
    "Grocery": ["Rice", "Milk", "Oil", "Sugar", "Bread"],
    "Books": ["Novel", "Comics", "Notebook", "Dictionary", "Magazine"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Wardrobe"]
}

# Function to generate random records
def generate_batch(start_id, batch_size):
    start_time = datetime(2025, 8, 20, 8, 0, 0)
    batch = []
    for i in range(batch_size):
        category = random.choice(list(categories.keys()))
        name = random.choice(categories[category])
        product_name = f"{name}_{start_id + i}"  # ensure unique name
        price = round(random.uniform(50, 50000), 2)
        timestamp = (start_time + timedelta(seconds=(start_id + i))).strftime('%Y-%m-%d %H:%M:%S')
        batch.append((start_id + i, product_name, category, price, timestamp))
    return batch

insert_query = """
INSERT INTO product (id, name, category, price, last_update)
VALUES (%s, %s, %s, %s, %s)
"""

total_records = 1_000_000  # Change to desired size (millions if needed)
batch_size = 10_000

try:
    conn = mysql.connector.connect(**mysql_config)
    if conn.is_connected():
        print("Connected to MySQL database")
        cursor = conn.cursor()

        current_id = 1
        while current_id <= total_records:
            batch = generate_batch(current_id, batch_size)
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f"Inserted batch up to ID {current_id + batch_size - 1}")
            current_id += batch_size

except Error as e:
    print(f"Error: {e}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")
