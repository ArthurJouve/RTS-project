from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
import random
import json
import time
from datetime import datetime

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

# Create Kafka producer
conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)
fake = Faker()

print("🚀 Generating and sending random network events... (Press Ctrl+C to stop)\n")

def generate_event():
    """Generate a random network event."""
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event_id": fake.uuid4(),
        "resource_type": random.choice(["server", "router", "switch", "firewall"]),
        "resource_id": random.randint(1, 100),
        "operational_status": random.choice(["critical_alarm", "major_alarm", "minor_alarm", "warning", "none"]),
        "resource_ip": fake.ipv4_private(),
        "region": random.choice(["EU-Central", "US-East", "AP-Southeast"]),
        "message": random.choice([
            "High memory usage detected",
            "CPU overload detected",
            "Packet loss increasing",
            "Network latency above threshold",
            "No issues detected"
        ]),
        "cpu_usage": round(random.uniform(10.0, 99.0), 2),
        "memory_usage": round(random.uniform(10.0, 99.0), 2),
        "latency_ms": round(random.uniform(1.0, 100.0), 2),
        "packet_loss": round(random.uniform(0.0, 0.5), 2)
    }
    return event

# Continuous event loop
try:
    while True:
        event = generate_event()
        event_json = json.dumps(event)
        producer.produce(topic, value=event_json.encode('utf-8'))
        producer.flush()
        print(f"📤 Sent event: {event_json}")
        time.sleep(random.uniform(1, 3))  # simulate random delay between events

except KeyboardInterrupt:
    print("\n🛑 Stopping producer.")
finally:
    producer.flush()
