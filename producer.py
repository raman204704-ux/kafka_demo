from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

for i in range(10):

    transaction = {
        "transaction_id": i,
        "user_id": 100 + i,
        "amount": 1000 * (i + 1)
    }

    producer.send(
        "transactions",
        value=transaction
    )

    print("Sent:", transaction)

    time.sleep(1)

producer.flush()
producer.close()