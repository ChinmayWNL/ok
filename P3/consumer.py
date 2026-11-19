from kafka import KafkaConsumer
import json

consumer =KafkaConsumer(
    "student-events",
    bootstrap_servers=[
        "localhost:29092",
        "localhost:39092",
        "localhost:49092"
    ],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="student-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Waiting for messages..\n")

for message in consumer:
    print("Consume:",message.value,"Messagge-Partition",message.partition,"message-offset",message.offset)


#docker exec kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic student-events --partitions 3 --replication-factor 3
