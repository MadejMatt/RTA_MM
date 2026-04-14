from kafka import KafkaConsumer, KafkaProducer
import json

def score_transaction(tx):
    
    score = 0
    rules = []

    R1 = tx.get('amount') > 3000.0
    R2 = tx.get('amount') > 1500.0 and tx['category'] == 'elektronika'
    R3 = tx.get('hour') < 6

    if R1:
        score += 3
        rules.append('R1')
    if R2:
        score += 2
        rules.append('R2')
    if R3:
        score += 2
        rules.append('R3')

    return score, rules


consumer = KafkaConsumer(
    'lab1', 
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    tran = message.value
    score = score_transaction(tran)[0]
    if score >= 3:
        alert_producer.send('alerts', value=f'alert {tran}')
        
alert_producer.flush()
alert_producer.close()
