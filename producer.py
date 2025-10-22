from confluent_kafka import Producer
import time

# Kafka broker configuration
conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)

topic = 'test-topic'

print("✅ Kafka Producer ready. Sending automatic messages...\n")

i = 0
try:
    while True:
        message = f"Automatic message {i}"
        producer.produce(topic, value=message.encode('utf-8'))
        producer.flush()
        print(f"☑️  Message sent: {message}")
        i += 1
        time.sleep(2)  # send one message every 2 seconds

except KeyboardInterrupt:
    print("\n🛑 Producer manually stopped.")
except Exception as e:
    print(f"⚠️ Error: {e}")
finally:
    producer.flush()
    print("👋 Shutting down Producer...")