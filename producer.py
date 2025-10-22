from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
bootstrap_servers = "kafka:9092"
topic = "test-topic"

# Ensure topic exists
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1)]

try:
    admin_client.create_topics(topic_list)
    print(f"✅ Topic '{topic}' created (if it didn't exist).")
except Exception as e:
    print(f"ℹ️ Topic may already exist: {e}")

# Create producer
conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

print("✅ Kafka Producer ready. Type messages to send (Ctrl+C to stop):\n")

# Manual message input loop
try:
    while True:
        message = input("> ")
        if not message.strip():
            continue  # skip empty messages
        producer.produce(topic, value=message.encode('utf-8'))
        producer.flush()
        print(f"☑️  Message sent: {message}")

except KeyboardInterrupt:
    print("\n🛑 Stopping producer.")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    producer.flush()
