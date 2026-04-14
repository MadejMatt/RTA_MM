from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0
window_size = 10

try:
    for message in consumer:
        tx = message.value
        store = tx.get('store', 'Unknown')
        amount = tx.get('amount', 0.0)

        store_counts[store] += 1
        total_amount[store] += amount
        msg_count += 1

        if msg_count % window_size == 0:
            print('---------------------------------------------------')
            for sklep, count in store_counts.most_common():
                print(f"Sklep: {sklep}, Liczba transakcji: {count}, Suma: {total_amount[sklep]:.2f} PLN")
            print('---------------------------------------------------')
            store_counts.clear()
            total_amount.clear()
except KeyboardInterrupt:
    print("\nZatrzymano konsumenta.")
finally:
    consumer.close()
