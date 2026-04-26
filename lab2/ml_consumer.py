from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('lab1', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

for message in consumer:
    tx = message.value

    features = {
        "amount": tx["amount"],
        "is_electronics": 1 if tx["category"] == "elektronika" else 0,
        "tx_per_minute": 5
    }

    response = requests.post(API_URL, json=features)
    result = response.json()

    if result["is_fraud"]:
        alert = {
            "timestamp": datetime.now().isoformat(),
            "transaction": tx,
            "fraud_probability": result["fraud_probability"]
        }
        alert_producer.send('alerts', alert)
        print(f"🚨 ALERT: {tx['tx_id']} - fraud_prob={result['fraud_probability']:.2%}")
