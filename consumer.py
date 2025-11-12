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
            # Décoder et parser l'événement
            event_data = msg.value().decode('utf-8')
            print(f"✉️  Received: {event_data}")
            
            try:
                # Parser le JSON de l'événement
                event = json.loads(event_data)
                
                # Construire la clé Redis
                key = f"resource:{event['resource_type']}:{event['resource_id']}"
                
                # Écrire dans Redis avec HSET
                redis_client.hset(key, mapping={
                    'timestamp': event['timestamp'],
                    'event_id': event['event_id'],
                    'resource_type': event['resource_type'],
                    'resource_id': str(event['resource_id']),
                    'operational_status': event['operational_status']
                })
                
                print(f"✅ Updated Redis: {key} -> {event['operational_status']}")
                
                # Committer l'offset seulement après succès Redis
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
