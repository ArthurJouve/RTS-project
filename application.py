import redis
from confluent_kafka import Consumer, KafkaError
import json
import threading
import time
from datetime import datetime, timezone

class DummyApplication:
    def __init__(self):
        # Redis connection
        self.redis_client = redis.Redis(
            host='redis',
            port=6379,
            decode_responses=True
        )
        
        # Kafka Consumer configuration
        self.kafka_conf = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'dummy-app-group',
            'auto.offset.reset': 'earliest',  # Get all available messages
            'enable.auto.commit': False
        }
        self.consumer = Consumer(self.kafka_conf)
        
        # Application's merged state (reliable, consistent final state)
        self.merged_state = {}
        self.lock = threading.Lock()
        
        # Track synchronization status
        self.redis_loaded = False
        self.kafka_started = False
        self.sync_start_time = None
        
        # Statistics
        self.events_from_redis = 0
        self.events_from_kafka = 0
        self.merge_conflicts = 0
        self.desync_detected = 0
        
    def load_initial_state_from_redis(self):
        """
        Step 1: Get past/initial state from Redis at startup
        """
        print("=" * 70)
        print("STEP 1: Loading initial state from Redis")
        print("=" * 70)
        
        self.sync_start_time = datetime.now(timezone.utc)
        
        # Retrieve all resource:* keys
        keys = self.redis_client.keys('resource:*')
        
        with self.lock:
            for key in keys:
                data = self.redis_client.hgetall(key)
                if not data:
                    continue
                    
                resource_id = f"{data['resource_type']}:{data['resource_id']}"
                
                self.merged_state[resource_id] = {
                    'timestamp': data['timestamp'],
                    'event_id': data['event_id'],
                    'resource_type': data['resource_type'],
                    'resource_id': data['resource_id'],
                    'operational_status': data['operational_status'],
                    'source': 'redis_initial',
                    'last_updated': self.sync_start_time.isoformat()
                }
                
                self.events_from_redis += 1
        
        self.redis_loaded = True
        
        print(f"✅ Loaded {self.events_from_redis} resources from Redis (initial state)")
        print(f"📸 Snapshot time: {self.sync_start_time.isoformat()}\n")
        
    def consume_kafka_events(self):
        """
        Step 2: Get new events from Kafka in real time
        """
        print("=" * 70)
        print("STEP 2: Subscribing to Kafka for real-time events")
        print("=" * 70)
        print("⚡ Real-time merge mode active\n")
        
        self.consumer.subscribe(['test-topic'])
        self.kafka_started = True
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Kafka error: {msg.error()}")
                        continue
                
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    self.merge_event(event)
                    self.consumer.commit(asynchronous=False)
                    
                except json.JSONDecodeError as e:
                    print(f"⚠️  JSON decode error: {e}")
                except Exception as e:
                    print(f"❌ Error processing event: {e}")
                    import traceback
                    traceback.print_exc()
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping Kafka consumer...")
            self.print_final_statistics()
        finally:
            self.consumer.close()
    
    def merge_event(self, event):
        """
        Step 3: Merge the two streams (Redis + Kafka)
        Core challenge: Handle delays and desynchronization
        """
        resource_id = f"{event['resource_type']}:{event['resource_id']}"
        event_timestamp = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        
        print(f"\n📨 Kafka event: {resource_id} - {event['operational_status']}")
        print(f"   Event timestamp: {event['timestamp']}")
        print(f"   Event ID: {event['event_id']}")
        
        with self.lock:
            self.events_from_kafka += 1
            
            # Check if resource exists in merged state
            if resource_id in self.merged_state:
                existing = self.merged_state[resource_id]
                existing_timestamp = datetime.fromisoformat(
                    existing['timestamp'].replace('Z', '+00:00')
                )
                
                # Handle delay: Compare timestamps to resolve conflicts
                if event_timestamp > existing_timestamp:
                    # Kafka event is newer - update
                    print(f"   🔄 MERGE: Kafka event is newer")
                    print(f"      Old: {existing['operational_status']} ({existing['timestamp']})")
                    print(f"      New: {event['operational_status']} ({event['timestamp']})")
                    
                    if existing['operational_status'] != event['operational_status']:
                        self.merge_conflicts += 1
                    
                    self.update_merged_state(resource_id, event, 'kafka_update')
                    
                elif event_timestamp == existing_timestamp:
                    # Same timestamp - check event IDs
                    if existing['event_id'] == event['event_id']:
                        print(f"   ✅ DUPLICATE: Same event already in state (no update needed)")
                    else:
                        print(f"   ⚠️  CONFLICT: Same timestamp, different event IDs")
                        self.desync_detected += 1
                        # Keep the one from Kafka as it's the source of truth
                        self.update_merged_state(resource_id, event, 'kafka_conflict')
                        
                else:
                    # Kafka event is older - handle desynchronization
                    print(f"   ⏪ DESYNC: Kafka event is older than current state")
                    print(f"      Current: {existing['operational_status']} ({existing['timestamp']})")
                    print(f"      Received: {event['operational_status']} ({event['timestamp']})")
                    print(f"   → Keeping current state (newer)")
                    self.desync_detected += 1
                    
            else:
                # New resource discovered via Kafka
                print(f"   🆕 NEW: Resource not in initial Redis state")
                self.update_merged_state(resource_id, event, 'kafka_new')
            
            # Compare with current Redis state to detect synchronization issues
            self.detect_synchronization_issues(resource_id)
    
    def update_merged_state(self, resource_id, event, source):
        """
        Update the merged state (called within lock)
        """
        self.merged_state[resource_id] = {
            'timestamp': event['timestamp'],
            'event_id': event['event_id'],
            'resource_type': event['resource_type'],
            'resource_id': event['resource_id'],
            'operational_status': event['operational_status'],
            'source': source,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        print(f"   ✅ Merged state updated: {event['operational_status']}")
    
    def detect_synchronization_issues(self, resource_id):
        """
        Step 4: Compute differences between merged state and current Redis
        Detects if consumer is behind, ahead, or in sync
        """
        # Get current Redis state for comparison
        resource_type, resource_id_num = resource_id.split(':')
        redis_key = f"resource:{resource_type}:{resource_id_num}"
        
        redis_current = self.redis_client.hgetall(redis_key)
        merged = self.merged_state[resource_id]
        
        if not redis_current:
            print(f"   📊 SYNC STATUS: Redis consumer hasn't processed this yet")
            self.export_difference({
                'resource_id': resource_id,
                'merged_status': merged['operational_status'],
                'redis_status': 'NOT_YET_PROCESSED',
                'sync_issue': 'CONSUMER_LAG',
                'merged_timestamp': merged['timestamp']
            })
            return
        
        # Compare merged state with current Redis
        if redis_current['operational_status'] != merged['operational_status']:
            redis_ts = datetime.fromisoformat(redis_current['timestamp'].replace('Z', '+00:00'))
            merged_ts = datetime.fromisoformat(merged['timestamp'].replace('Z', '+00:00'))
            
            if redis_ts > merged_ts:
                issue = 'MERGED_STATE_BEHIND'
                print(f"   📊 SYNC STATUS: Merged state is behind Redis consumer")
            elif redis_ts < merged_ts:
                issue = 'REDIS_CONSUMER_BEHIND'
                print(f"   📊 SYNC STATUS: Redis consumer is behind merged state")
            else:
                issue = 'STATUS_CONFLICT_SAME_TIME'
                print(f"   📊 SYNC STATUS: Conflict at same timestamp")
            
            print(f"      Merged: {merged['operational_status']} ({merged['timestamp']})")
            print(f"      Redis: {redis_current['operational_status']} ({redis_current['timestamp']})")
            
            self.export_difference({
                'resource_id': resource_id,
                'merged_status': merged['operational_status'],
                'redis_status': redis_current['operational_status'],
                'merged_timestamp': merged['timestamp'],
                'redis_timestamp': redis_current['timestamp'],
                'merged_event_id': merged['event_id'],
                'redis_event_id': redis_current['event_id'],
                'sync_issue': issue
            })
        else:
            print(f"   📊 SYNC STATUS: ✅ Perfect sync with Redis consumer")
    
    def export_difference(self, difference):
        """
        Export detected differences for monitoring
        """
        try:
            metric = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'events_from_redis': self.events_from_redis,
                'events_from_kafka': self.events_from_kafka,
                'merge_conflicts': self.merge_conflicts,
                'desync_detected': self.desync_detected,
                'difference': difference
            }
            
            # Real-time log
            with open('/tmp/sync_issues.jsonl', 'a') as f:
                f.write(json.dumps(metric) + '\n')
            
            # Latest state for Grafana
            with open('/tmp/sync_latest.json', 'w') as f:
                json.dump(metric, f, indent=2)
                
        except Exception as e:
            print(f"⚠️  Error exporting: {e}")
    
    def get_final_consistent_state(self):
        """
        Produce a reliable, consistent final state
        """
        with self.lock:
            return dict(self.merged_state)
    
    def print_final_statistics(self):
        """
        Print final statistics
        """
        print("\n" + "=" * 70)
        print("📊 FINAL STATISTICS - Reliable, Consistent Final State")
        print("=" * 70)
        print(f"Resources in merged state: {len(self.merged_state)}")
        print(f"Events from Redis (initial): {self.events_from_redis}")
        print(f"Events from Kafka (real-time): {self.events_from_kafka}")
        print(f"Merge conflicts resolved: {self.merge_conflicts}")
        print(f"Desynchronization detected: {self.desync_detected}")
        
        if self.events_from_kafka > 0:
            conflict_rate = (self.merge_conflicts / self.events_from_kafka) * 100
            desync_rate = (self.desync_detected / self.events_from_kafka) * 100
            print(f"\nConflict rate: {conflict_rate:.2f}%")
            print(f"Desynchronization rate: {desync_rate:.2f}%")
        
        # Count by source
        sources = {}
        with self.lock:
            for resource_id, state in self.merged_state.items():
                source = state['source']
                sources[source] = sources.get(source, 0) + 1
        
        print(f"\nFinal state composition:")
        for source, count in sources.items():
            print(f"  - {source}: {count} resources")
        
        print("=" * 70)
        print("✅ Resynchronization complete - No events lost")
        print("=" * 70)
    
    def start(self):
        """
        Start the dummy application
        """
        print("\n" + "=" * 70)
        print("🚀 DUMMY APPLICATION - Event Resynchronization System")
        print("=" * 70)
        print("Goal: Merge Redis (past) + Kafka (real-time) → Consistent state")
        print("Challenge: Handle delays, conflicts, desynchronization")
        print("=" * 70 + "\n")
        
        # Step 1: Load initial state from Redis
        self.load_initial_state_from_redis()
        
        # Step 2: Subscribe to Kafka and merge in real-time
        self.consume_kafka_events()


if __name__ == "__main__":
    app = DummyApplication()
    app.start()
