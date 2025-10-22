from confluent_kafka import Producer

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)
topic = 'test-topic'

print("✅ Kafka Producer ready. Waiting for data:\n")

try:
    while True:
        message = input("> ")
        if not message.strip():
            continue

        producer.produce(topic, value=message)
        producer.flush()
        print(f"☑️ Message sent: {message}")

except KeyboardInterrupt:
    print("\n🛑 Stopping producer.")
finally:
    producer.flush()