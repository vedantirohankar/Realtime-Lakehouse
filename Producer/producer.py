from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

fake = Faker("en_IN")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

products = [
    "Laptop",
    "Phone",
    "Monitor",
    "TV",
    "Keyboard",
    "Mouse",
    "Tablet"
]

while True:

    sale = {
        "OrderID": random.randint(100000,999999),
        "Customer": fake.name(),
        "City": fake.city(),
        "Product": random.choice(products),
        "Quantity": random.randint(1,5),
        "Price": random.randint(5000,120000),
        "Timestamp": str(fake.date_time_this_month())
    }

    producer.send("sales", sale)
    producer.flush()

    print(sale)

    time.sleep(1)