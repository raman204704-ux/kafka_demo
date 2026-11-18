from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    group_id="fraud-detection-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Fraud detection service started...")

for message in consumer:

    transaction = message.value

    amount = transaction["amount"]

    if amount > 50000:
        risk = "HIGH"
    elif amount > 20000:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    print(
        f"Transaction {transaction['transaction_id']} "
        f"Amount={amount} "
        f"Risk={risk}"
    )