from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    transaction = message.value
    if transaction['amount'] > 3000:
        risk_level = 'HIGH'
    elif transaction['amount'] > 1000:
        risk_level = 'MEDIUM'
    else:
        risk_level = 'LOW'
    transaction['risk_level'] = risk_level
    print(transaction)
    
