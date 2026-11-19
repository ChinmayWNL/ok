
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda x: json.dumps(x).encode()
        )
        print("Kafka Connected!")
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka...")
        time.sleep(3)

cities = ["Mumbai", "Pune", "Nashik", "Nagpur"]

while True:
    data = {
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "city": random.choice(cities),
        "pm25": round(random.uniform(10, 180), 2),
        "pm10": round(random.uniform(20, 250), 2),
        "temperature": round(random.uniform(24, 38), 2)
    }

    producer.send("air_quality", data)
    producer.flush()
    print("Sent:", data)
    time.sleep(2)
