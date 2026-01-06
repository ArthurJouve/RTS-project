"""
Zero-Loss Verification Application for Kafka-Redis Synchronization

This application verifies that a Kafka consumer does not lose any events
during restart by:
1. Capturing a Redis snapshot while the consumer is paused
2. Reading Kafka events from the same offset as the consumer
3. Applying the Kafka delta to the snapshot
4. Verifying synchronization with the current Redis state
"""

import redis
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
import time
import random
from datetime import datetime, timezone, timedelta


class ConsumerAPIClient:
    """
    Controls the Kafka consumer via Redis signals.
    
    Uses Redis keys to send pause/resume commands to the consumer.
    """
    
    def __init__(self, redis_client):
        """
        Initialize the consumer controller.
        
        Args:
            redis_client: Redis client instance for sending control signals
        """
        self.redis_client = redis_client
        self.control_key = "consumer_control"
        self.api_available = True
        print("✅ Redis-based consumer control initialized")
    
    def stop_consumer(self):
        """Send pause signal to consumer via Redis."""
        try:
            self.redis_client.set(self.control_key, "pause")
            time.sleep(1.0)
            return True
        except Exception as e:
            print(f"❌ Failed to stop consumer: {e}")
            return False
    
    def get_current_offset(self):
        """Retrieve the last committed Kafka offset from consumer."""
        try:
            offset_str = self.redis_client.get("consumer_current_offset")
            return {"offset": offset_str or "{}"}
        except Exception as e:
            print(f"❌ Failed to get offset: {e}")
            return None
    
    def restart_consumer(self):
        """Send resume signal to consumer via Redis."""
        try:
            self.redis_client.set(self.control_key, "run")
            time.sleep(0.5)
            return True
        except Exception as e:
            print(f"❌ Failed to restart consumer: {e}")
            return False


class DummyApplication:
    """
    Main application for zero-loss verification.
    
    Implements the professor's approach:
    - Stop consumer
    - Scan Redis snapshot (captures historical state)
    - Restart consumer
    - Read Kafka delta (new events)
    - Apply delta to snapshot
    - Verify against current Redis state
    """
    
    def __init__(self):
        """Initialize application with Redis connection and configuration."""
        # Redis connection pool
        self.redispool = redis.ConnectionPool(
            host="redis",
            port=6379,
            decode_responses=True,
            max_connections=10,
        )
        self.redisclient = redis.Redis(connection_pool=self.redispool)
        
        # Consumer control client
        self.api_client = ConsumerAPIClient(self.redisclient)
        
        # Internal state
        self.redis_snapshot = {}
        self.snapshot_offset = {}
        self.snapshot_timestamp = None
        self.total_verifications_performed = 0
        self.verification_reports = []
        
        # Random restart configuration (in seconds)
        self.min_restart_interval = 10
        self.max_restart_interval = 30
        self.min_pause_duration = 5
        self.max_pause_duration = 10
    
    def scan_full_redis_snapshot(self):
        """
        Scan all Redis keys to capture complete resource state.
        
        This snapshot represents the consolidated network state at time T0
        when the consumer is paused.
        
        Returns:
            dict: Snapshot with resource_id as key and resource data as value
        """
        snapshot = {}
        cursor = 0
        scan_start = time.time()
        
        # Scan all resource keys
        while True:
            cursor, keys = self.redisclient.scan(
                cursor=cursor,
                match="resource:*",
                count=1000,
            )
            
            if keys:
                pipe = self.redisclient.pipeline(transaction=False)
                for key in keys:
                    pipe.hgetall(key)
                results = pipe.execute()
                
                for key, data in zip(keys, results):
                    if data:
                        resource_id = f"{data.get('resource_type')}:{data.get('resource_id')}"
                        snapshot[resource_id] = data
            
            if cursor == 0:
                break
        
        scan_duration = time.time() - scan_start
        self.snapshot_timestamp = datetime.now(timezone.utc)
        
        print(f"📸 Snapshot: {len(snapshot)} resources ({scan_duration:.3f}s)")
        
        return snapshot
    
    def read_kafka_from_offset(self, offset_map, duration=0.5):
        """
        Read Kafka events from specified offset for given duration.
        
        Creates a temporary consumer to read the same events that the
        Redis consumer will process after restart.
        
        Args:
            offset_map: Dictionary mapping partition to offset
            duration: How long to read events (seconds)
            
        Returns:
            list: Captured Kafka events
        """
        unique_group_id = f"app-verify-{int(time.time() * 1000)}"
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': unique_group_id,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
        })
        
        consumer.subscribe(['test-topic'])
        
        # Wait for partition assignment
        assignment = None
        for i in range(20):
            consumer.poll(0.5)
            assignment = consumer.assignment()
            if assignment:
                break
        
        if not assignment:
            print("❌ Failed to get Kafka partition assignment")
            consumer.close()
            return []
        
        # Seek to captured offsets
        if offset_map:
            for tp in assignment:
                partition_key = str(tp.partition)
                if partition_key in offset_map:
                    offset = offset_map[partition_key]
                    consumer.seek(TopicPartition(tp.topic, tp.partition, offset))
        
        # Read events for specified duration
        events = []
        end_time = datetime.now(timezone.utc) + timedelta(seconds=duration)
        
        while datetime.now(timezone.utc) < end_time:
            remaining = (end_time - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                break
            
            msg = consumer.poll(min(0.05, max(0.01, remaining)))
            
            if msg and not msg.error():
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
        
        consumer.close()
        print(f"📖 Kafka: {len(events)} events captured")
        
        return events
    
    def apply_kafka_delta_to_snapshot(self, kafka_events):
        """
        Apply Kafka events to Redis snapshot.
        
        Reproduces the consumer's logic: for each event, update the resource
        if the event is more recent than the existing data.
        
        Args:
            kafka_events: List of Kafka events to apply
            
        Returns:
            dict: Updated application state (snapshot + delta)
        """
        app_state = self.redis_snapshot.copy()
        
        # Sort events by timestamp
        kafka_events.sort(
            key=lambda e: datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
        )
        
        updates = 0
        new_resources = 0
        
        for event in kafka_events:
            resource_id = f"{event['resource_type']}:{event['resource_id']}"
            event_time = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
            
            if resource_id not in app_state:
                app_state[resource_id] = event
                new_resources += 1
            else:
                existing_time = datetime.fromisoformat(
                    app_state[resource_id]["timestamp"].replace("Z", "+00:00")
                )
                
                if event_time >= existing_time:
                    app_state[resource_id] = event
                    updates += 1
        
        print(f"🔄 Delta applied: {new_resources} new, {updates} updated")
        
        return app_state
    
    def verify_final_state(self, app_final_state, t0):
        """
        Compare application state with current Redis state.
        
        Verifies that the application correctly reproduced the consumer's
        processing by comparing final states.
        
        Args:
            app_final_state: Application's computed state (snapshot + delta)
            t0: Verification start timestamp
            
        Returns:
            dict: Verification report with metrics
        """
        time.sleep(0.5)
        
        # Scan current Redis state
        current_redis_state = {}
        cursor = 0
        
        while True:
            cursor, keys = self.redisclient.scan(cursor=cursor, match="resource:*", count=1000)
            if keys:
                pipe = self.redisclient.pipeline(transaction=False)
                for key in keys:
                    pipe.hgetall(key)
                results = pipe.execute()
                
                for key, data in zip(keys, results):
                    if data:
                        resource_id = f"{data.get('resource_type')}:{data.get('resource_id')}"
                        current_redis_state[resource_id] = data
            
            if cursor == 0:
                break
        
        # Compare states
        missing_in_app = []
        missing_in_redis = []
        outdated = []
        correct = 0
        
        for resource_id, redis_data in current_redis_state.items():
            if resource_id not in app_final_state:
                missing_in_app.append(resource_id)
            else:
                app_data = app_final_state[resource_id]
                
                try:
                    redis_time = datetime.fromisoformat(redis_data["timestamp"].replace("Z", "+00:00"))
                    app_time = datetime.fromisoformat(app_data["timestamp"].replace("Z", "+00:00"))
                    
                    if app_time < redis_time:
                        outdated.append({
                            "resource_id": resource_id,
                            "timediff_seconds": (redis_time - app_time).total_seconds(),
                        })
                    else:
                        correct += 1
                except (ValueError, KeyError):
                    outdated.append({"resource_id": resource_id, "reason": "timestamp_error"})
        
        for resource_id in app_final_state.keys():
            if resource_id not in current_redis_state:
                missing_in_redis.append(resource_id)
        
        total_issues = len(missing_in_app) + len(missing_in_redis) + len(outdated)
        
        # Results
        print(f"\n📊 Verification #{self.total_verifications_performed + 1}")
        print(f"✅ Correct: {correct} | ⚠️ Outdated: {len(outdated)} | ❌ Missing: {len(missing_in_app) + len(missing_in_redis)}")
        
        if total_issues == 0:
            print("🎉 PERFECT SYNCHRONIZATION")
        elif len(outdated) <= 15 and len(missing_in_app) == 0 and len(missing_in_redis) == 0:
            print(f"✅ NEAR-PERFECT ({len(outdated)} outdated from post-capture events)")
        
        # Increment verification counter
        self.total_verifications_performed += 1
        
        # Create report
        report = {
            "verification_number": self.total_verifications_performed,
            "timestamp": t0.isoformat(),
            "duration_seconds": (datetime.now(timezone.utc) - t0).total_seconds(),
            "snapshot": {
                "timestamp": self.snapshot_timestamp.isoformat() if self.snapshot_timestamp else None,
                "size": len(self.redis_snapshot),
                "offsets": self.snapshot_offset,
            },
            "metrics": {
                "correctly_synchronized": correct,
                "missing_in_app": len(missing_in_app),
                "missing_in_redis": len(missing_in_redis),
                "outdated_in_app": len(outdated),
                "total_issues": total_issues,
            },
            "status": "PERFECT" if total_issues == 0 else "ISSUES-DETECTED",
        }
        
        # Save reports
        with open("current_verification_report.json", "w") as f:
            json.dump(report, f, indent=2)
        
        with open("verification_history.jsonl", "a") as f:
            f.write(json.dumps(report) + "\n")
        
        self.verification_reports.append(report)
        
        return report
    
    def trigger_random_consumer_restart(self):
        """
        Execute one cycle of consumer restart verification.
        
        Randomly waits, then pauses consumer for random duration,
        captures snapshot, restarts, and verifies synchronization.
        
        Returns:
            bool: True if cycle completed successfully
        """
        # Random wait before restart
        wait_time = random.uniform(self.min_restart_interval, self.max_restart_interval)
        print(f"\n⏰ Next verification in {wait_time:.1f}s...")
        time.sleep(wait_time)
        
        # Random pause duration
        pause_duration = random.uniform(self.min_pause_duration, self.max_pause_duration)
        
        print(f"\n🎲 Consumer restart (pause: {pause_duration:.1f}s)")
        
        # Stop consumer
        if not self.api_client.stop_consumer():
            return False
        time.sleep(0.5)
        
        # Capture Redis snapshot
        self.redis_snapshot = self.scan_full_redis_snapshot()
        
        # Get current offset
        offset_data = self.api_client.get_current_offset()
        if offset_data and "offset" in offset_data:
            try:
                self.snapshot_offset = json.loads(offset_data["offset"])
            except:
                self.snapshot_offset = {}
        else:
            self.snapshot_offset = {}
        
        # Keep consumer paused for remaining duration
        remaining_pause = pause_duration - 0.5
        if remaining_pause > 0:
            time.sleep(remaining_pause)
        
        # Restart consumer
        if not self.api_client.restart_consumer():
            return False
        
        # Wait for consumer ready
        self.wait_for_consumer_kafka_ready(timeout=3.0)
        
        t0 = datetime.now(timezone.utc)
        
        # Read Kafka events in parallel
        kafka_events = self.read_kafka_from_offset(self.snapshot_offset, duration=0.5)
        
        # Apply delta to snapshot
        app_final_state = self.apply_kafka_delta_to_snapshot(kafka_events)
        
        # Verify synchronization
        self.verify_final_state(app_final_state, t0)
        
        return True
    
    def wait_for_consumer_kafka_ready(self, timeout=5.0):
        """
        Wait for consumer to signal readiness via Redis.
        
        Args:
            timeout: Maximum wait time in seconds
            
        Returns:
            bool: True if consumer became ready, False if timeout
        """
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if self.redisclient.get("consumer_kafka_ready"):
                return True
            time.sleep(0.05)
        
        print(f"⚠️ Consumer ready timeout")
        return False
    
    def start(self):
        """
        Main application loop.
        
        Continuously triggers random consumer restarts and verifications
        until interrupted by user.
        """
        print("=" * 70)
        print("🚀 Zero-Loss Verification Application")
        print(f"   Restart interval: {self.min_restart_interval}-{self.max_restart_interval}s")
        print(f"   Pause duration: {self.min_pause_duration}-{self.max_pause_duration}s")
        print("=" * 70)
        
        try:
            while True:
                if self.api_client.api_available:
                    success = self.trigger_random_consumer_restart()
                    
                    if not success:
                        print("⚠️ Restart failed, retrying in 5s...")
                        time.sleep(5)
                else:
                    print("⚠️ Consumer control not available")
                    time.sleep(10)
        
        except KeyboardInterrupt:
            print("\n🛑 Application stopped")
            print(f"\n📊 Total verifications: {self.total_verifications_performed}")
            
            if self.verification_reports:
                perfect = sum(1 for r in self.verification_reports if r["status"] == "PERFECT")
                print(f"   ✅ Perfect: {perfect} | ⚠️ Issues: {len(self.verification_reports) - perfect}")
            
            print("\n📄 Reports: current_verification_report.json")
            print("📜 History: verification_history.jsonl")


if __name__ == "__main__":
    app = DummyApplication()
    app.start()
