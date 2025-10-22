from confluent_kafka import Producer

# Kafka configuration
conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)
topic = 'test-topic'

print("✅ Kafka Producer ready. Type a message and press Enter:\n")

try:
    while True:
        message = input("> ")
        if not message.strip():
            continue  # skip empty lines

        producer.produce(topic, value=message)
        producer.flush()
        print(f"☑️ Message sent: {message}")

except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")
finally:
    producer.flush()
