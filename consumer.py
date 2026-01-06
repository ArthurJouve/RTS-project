"""
Kafka Consumer with Redis Control and State Persistence

This consumer reads network monitoring events from Kafka and stores them in Redis.

Features:
- Pause/resume control via Redis
- Offset persistence for restart recovery
- Event processing with Redis storage
"""

from confluent_kafka import Consumer, KafkaError
import redis
import json
import time
from datetime import datetime, timezone


# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'python-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Redis client for state management and control
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Initialize Kafka consumer and subscribe to topic
consumer = Consumer(conf)
consumer.subscribe(['test-topic'])
print("⏲ Waiting for messages...\n")

# Processing metrics
events_processed = 0

# Signal readiness to application (with TTL for auto-cleanup)
redis_client.set("consumer_kafka_ready", "1")
redis_client.expire("consumer_kafka_ready", 10)
print("✅ Consumer ready to read Kafka")

# Redis-based control
CONTROL_KEY = "consumer_control"
redis_client.set(CONTROL_KEY, "run")  # Initial state: running

try:
    while True:
        # Check for pause command from application
        control = redis_client.get(CONTROL_KEY)
        
        if control == "pause":
            if consumer.assignment():
                print("⏸️  Consumer PAUSED by control signal")
                consumer.pause(consumer.assignment())
                
                # Save current offset for application to read
                partitions = consumer.assignment()
                if partitions:
                    committed = consumer.committed(partitions, timeout=5.0)
                    offset_info = {}
                    for p in committed:
                        if p and p.offset >= 0:
                            offset_info[str(p.partition)] = p.offset
                    redis_client.set("consumer_current_offset", json.dumps(offset_info))
                    print(f"💾 Saved offsets: {offset_info}")
                
                # Wait for resume signal
                while redis_client.get(CONTROL_KEY) == "pause":
                    consumer.poll(1.0)  # Keep connection alive
                    time.sleep(0.5)
                
                # Resume processing
                consumer.resume(consumer.assignment())
                print("▶️  Consumer RESUMED")
                redis_client.set("consumer_kafka_ready", "1")
                redis_client.expire("consumer_kafka_ready", 10)
        
        # Poll for new messages
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Error: {msg.error()}")
        else:
            event_data = msg.value().decode('utf-8')
            print(f"✉️  Received: {event_data}")

            try:
                # Parse event and store in Redis
                event = json.loads(event_data)
                key = f"resource:{event['resource_type']}:{event['resource_id']}"
                redis_client.hset(key, mapping=event)
                print(f"✅ Updated Redis: {key}")
                
                # Commit offset synchronously to ensure durability
                consumer.commit(asynchronous=False)
                events_processed += 1

            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️  Error: {e}")

except KeyboardInterrupt:
    print(f"\n🛑 Consumer stopped. Total events: {events_processed}")
finally:
    consumer.close()
    