from confluent_kafka import Consumer, KafkaError

# Configuration du consumer Kafka
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'python-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

print('⏲ Waiting for messages \n')

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print('Erreur :', msg.error())
    else:
        print('✉️ Received :', msg.value().decode('utf-8'))
