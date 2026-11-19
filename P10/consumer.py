
import json
import time
import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

while True:
    try:
        db = clickhouse_connect.get_client(
            host="clickhouse", port=8123
        )
        db.command("""
            CREATE TABLE IF NOT EXISTS default.air_quality (
                event_time DateTime,
                city String,
                pm25 Float32,
                pm10 Float32,
                temperature Float32
            ) ENGINE = MergeTree()
            ORDER BY (city, event_time)
        """)
        print("ClickHouse Connected!")
        break
    except Exception as e:
        print("Waiting for ClickHouse...", e)
        time.sleep(3)

while True:
    try:
        consumer = KafkaConsumer(
            "air_quality",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode())
        )
        print("Kafka Connected!")
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka...")
        time.sleep(3)

for msg in consumer:
    d = msg.value

    db.insert("air_quality", [[
        d["event_time"], d["city"], d["pm25"],
        d["pm10"], d["temperature"]
    ]], column_names=[
        "event_time", "city", "pm25", "pm10", "temperature"
    ])

    print("Inserted:", d)
