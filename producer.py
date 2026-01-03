"""
Network Event Generator and Kafka Producer

Generates network monitoring events (servers, routers, switches, and firewalls). Events include metrics like CPU,
memory, latency, and packet loss with configurable alarm thresholds.

Features:
- FQDN-style resource identifiers
- Weighted problem distribution (70% normal, 30% issues)
- Single-metric problems per event (avoids unrealistic multi-alarm scenarios)
- Severity-based thresholds (critical, major, minor, warning)
"""

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


# Ensure topic exists before producing
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1)]

try:
    admin_client.create_topics(topic_list)
    print(f"✅ Topic '{topic}' created (if it didn't exist).")
except Exception as e:
    print(f"ℹ️ Topic may already exist: {e}")


# Initialize Kafka producer and fake data generator
conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)
fake = Faker()

print("🚀 Generating and sending random network events... (Press Ctrl+C to stop)\n")


def generate_event():
    """
    Generate a realistic network monitoring event.
    Most events are normal (70%), while some have problems (30%) affecting a single metric.
    
    Returns:
        dict: Network event with identification, operational status, and telemetry
    """
    # Resource identification
    resource_type = random.choice(["server", "router", "switch", "firewall"])
    id_num = random.randint(1, 20)
    region = random.choice(["EU-West", "US-East", "EU-East"])
    
    # Generate FQDN-style identifier (e.g., server-5.eu-west.ensea.com)
    fqdn = f"{resource_type}-{id_num}.{region.lower()}.ensea.com"
    
    # Alarm thresholds based on industry standards
    THRESHOLDS = {
        'cpu_critical': 90,
        'cpu_major': 80,
        'cpu_minor': 70,
        
        'memory_critical': 95,
        'memory_major': 85,
        'memory_minor': 75,
        
        'latency_critical': 100,
        'latency_major': 80,
        'latency_minor': 50,
        
        'packet_loss_critical': 0.5,
        'packet_loss_major': 0.3,
        'packet_loss_minor': 0.15
    }
    
    # Weighted decision: 70% normal events, 30% with problems
    has_problem = random.choices([True, False], weights=[30, 70], k=1)[0]
    
    if has_problem:
        # Select ONE metric to be problematic (realistic behavior)
        problem_metric = random.choice(['cpu', 'memory', 'latency', 'packet_loss'])
        
        # Choose severity level (weighted: fewer critical alarms)
        severity = random.choices(
            ['critical_alarm', 'major_alarm', 'minor_alarm', 'warning'],
            weights=[3, 7, 20, 20],
            k=1
        )[0]
        
        # Generate metrics based on the problematic metric and severity
        if problem_metric == 'cpu':
            if severity == 'critical_alarm':
                cpu_usage = round(random.uniform(90, 99), 2)
                operational_status = 'critical_alarm'
            elif severity == 'major_alarm':
                cpu_usage = round(random.uniform(80, 89), 2)
                operational_status = 'major_alarm'
            elif severity == 'minor_alarm':
                cpu_usage = round(random.uniform(70, 79), 2)
                operational_status = 'minor_alarm'
            else:
                cpu_usage = round(random.uniform(60, 69), 2)
                operational_status = 'warning'
            
            message = "CPU overload detected"
            # Other metrics remain in normal range
            memory_usage = round(random.uniform(20, 60), 2)
            latency_ms = round(random.uniform(5, 30), 2)
            packet_loss = round(random.uniform(0.0, 0.1), 2)
            
        elif problem_metric == 'memory':
            if severity == 'critical_alarm':
                memory_usage = round(random.uniform(95, 99), 2)
                operational_status = 'critical_alarm'
            elif severity == 'major_alarm':
                memory_usage = round(random.uniform(85, 94), 2)
                operational_status = 'major_alarm'
            elif severity == 'minor_alarm':
                memory_usage = round(random.uniform(75, 84), 2)
                operational_status = 'minor_alarm'
            else:
                memory_usage = round(random.uniform(65, 74), 2)
                operational_status = 'warning'
            
            message = "High memory usage detected"
            # Other metrics remain in normal range
            cpu_usage = round(random.uniform(20, 60), 2)
            latency_ms = round(random.uniform(5, 30), 2)
            packet_loss = round(random.uniform(0.0, 0.1), 2)
            
        elif problem_metric == 'latency':
            if severity == 'critical_alarm':
                latency_ms = round(random.uniform(100, 150), 2)
                operational_status = 'critical_alarm'
            elif severity == 'major_alarm':
                latency_ms = round(random.uniform(80, 99), 2)
                operational_status = 'major_alarm'
            elif severity == 'minor_alarm':
                latency_ms = round(random.uniform(50, 79), 2)
                operational_status = 'minor_alarm'
            else:
                latency_ms = round(random.uniform(30, 49), 2)
                operational_status = 'warning'
            
            message = "Network latency above threshold"
            # Other metrics remain in normal range
            cpu_usage = round(random.uniform(20, 60), 2)
            memory_usage = round(random.uniform(20, 60), 2)
            packet_loss = round(random.uniform(0.0, 0.1), 2)
            
        else:  # packet_loss
            if severity == 'critical_alarm':
                packet_loss = round(random.uniform(0.5, 1.0), 2)
                operational_status = 'critical_alarm'
            elif severity == 'major_alarm':
                packet_loss = round(random.uniform(0.3, 0.49), 2)
                operational_status = 'major_alarm'
            elif severity == 'minor_alarm':
                packet_loss = round(random.uniform(0.15, 0.29), 2)
                operational_status = 'minor_alarm'
            else:
                packet_loss = round(random.uniform(0.08, 0.14), 2)
                operational_status = 'warning'
            
            message = "Packet loss increasing"
            # Other metrics remain in normal range
            cpu_usage = round(random.uniform(20, 60), 2)
            memory_usage = round(random.uniform(20, 60), 2)
            latency_ms = round(random.uniform(5, 30), 2)
    
    else:
        # No problem - all metrics are in normal operational range
        operational_status = 'none'
        message = "No issues detected"
        cpu_usage = round(random.uniform(10, 50), 2)
        memory_usage = round(random.uniform(15, 50), 2)
        latency_ms = round(random.uniform(1, 25), 2)
        packet_loss = round(random.uniform(0.0, 0.05), 2)
    
    # Construct event with all fields
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event_id": fake.uuid4(),

        # Identification fields
        "resource_type": resource_type,
        "resource_id": id_num,
        "resource_fqdn": fqdn,
        
        # Operational data
        "operational_status": operational_status,
        "resource_ip": fake.ipv4_private(),
        "region": region,
        "message": message,

        # Telemetry metrics
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "latency_ms": latency_ms,
        "packet_loss": packet_loss
    }
    return event


# Continuous event generation loop
try:
    while True:
        event = generate_event()
        event_json = json.dumps(event)
        
        # Produce to Kafka and flush immediately (ensures delivery)
        producer.produce(topic, value=event_json.encode('utf-8'))
        producer.flush()
        
        print(f"📤 Sent event: {event_json}")
        
        # Generate ~10 events per second
        time.sleep(0.1)

except KeyboardInterrupt:
    print("\n🛑 Stopping producer.")
finally:
    # Ensure all pending messages are sent before exit
    producer.flush()
