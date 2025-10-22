from confluent_kafka import Producer

# Local broker
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

topic = 'test-topic'

print("✅ Kafka Producer ready. Waiting for data :\n")

try:
    while True:
        message = input("> ") 
        if not message.strip(): 
            continue

        producer.produce(topic, value=message)
        producer.flush() 
        print(f"☑️  Message envoyé : {message}")

except KeyboardInterrupt:
    print("\n🛑 Stop the producer.")
except Exception as e:
    print("Erreur :", e)
finally:
    producer.flush()
