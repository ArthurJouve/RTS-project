from confluent_kafka import Consumer, KafkaError
import redis
import json
import time
from datetime import datetime, timezone

# Kafka Configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'python-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Redis connection
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# ------------------------------------------------------------------
# Increment restart count (signal to application that we restarted)
# ------------------------------------------------------------------
restart_count = redis_client.get("consumer_restart_count")
if restart_count:
    new_count = int(restart_count) + 1
    redis_client.set("consumer_restart_count", new_count)
    print(f"🔄 Consumer restarted (count={new_count})")
else:
    redis_client.set("consumer_restart_count", 1)
    print("🆕 Consumer started for the first time (count=1)")

# Initialize consumer
consumer = Consumer(conf)
consumer.subscribe(['test-topic'])
print("⏲ Waiting for messages...\n")

# Statistics
events_processed = 0
last_heartbeat_update = time.time()
HEARTBEAT_INTERVAL = 1.0  # Update heartbeat every second

# ------------------------------------------------------------------
# NOUVEAU : Signal que le consumer est prêt à lire Kafka
# ------------------------------------------------------------------
print("🚀 Consumer ready to read from Kafka - setting ready flag...")
redis_client.set("consumer_kafka_ready", "1")
redis_client.expire("consumer_kafka_ready", 10)  # Expire après 10s
print("✅ Flag 'consumer_kafka_ready' set - application can start capture now")

try:
    while True:
        msg = consumer.poll(1.0)
        
        # Update heartbeat periodically even when no messages
        current_time = time.time()
        if current_time - last_heartbeat_update >= HEARTBEAT_INTERVAL:
            redis_client.set(
                "consumer_heartbeat",
                datetime.now(timezone.utc).isoformat()
            )
            last_heartbeat_update = current_time
        
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
                event = json.loads(event_data)
                key = f"resource:{event['resource_type']}:{event['resource_id']}"

                # Write ALL fields from the event to Redis
                redis_client.hset(key, mapping=event)

                print(f"✅ Updated Redis: {key}")
                consumer.commit(asynchronous=False)
                
                events_processed += 1
                
                # Update heartbeat after processing event
                redis_client.set(
                    "consumer_heartbeat",
                    datetime.now(timezone.utc).isoformat()
                )
                last_heartbeat_update = current_time

            except json.JSONDecodeError as e:
                print(f"⚠️  JSON decode error: {e}")
            except KeyError as e:
                print(f"⚠️  Missing field in event: {e}")
            except Exception as e:
                print(f"❌ Redis error: {e}")

except KeyboardInterrupt:
    print(f"\n🛑 Consumer stopped. Total events processed: {events_processed}")
finally:
    consumer.close()
