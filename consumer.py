from confluent_kafka import Consumer, KafkaError
import redis
import json

# Kafka Configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'python-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Redis connection
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

print("⏲ Waiting for messages...\n")

try:
    while True:
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
                event = json.loads(event_data)
                key = f"resource:{event['resource_type']}:{event['resource_id']}"

                # Write ALL fields from the event to Redis
                redis_client.hset(key, mapping=event)

                print(f"✅ Updated Redis: {key}")
                consumer.commit(asynchronous=False)

            except json.JSONDecodeError as e:
                print(f"⚠️  JSON decode error: {e}")
            except KeyError as e:
                print(f"⚠️  Missing field in event: {e}")
            except Exception as e:
                print(f"❌ Redis error: {e}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")
finally:
    consumer.close()
