import redis
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
import time
from datetime import datetime, timezone, timedelta


class DummyApplication:
    def __init__(self):
        # Redis connection with connection pooling
        self.redispool = redis.ConnectionPool(
            host="redis",
            port=6379,
            decode_responses=True,
            max_connections=10,
        )
        self.redisclient = redis.Redis(connection_pool=self.redispool)

        # Keys for tracking consumer state
        self.redis_consumer_restart_count_key = "consumer_restart_count"
        self.redis_consumer_heartbeat_key = "consumer_heartbeat"
        self.redis_consumer_kafka_ready_key = "consumer_kafka_ready"

        # Internal state
        self.last_known_restart_count = None
        self.total_verifications_performed = 0
        self.verification_reports = []
        
        # Pre-initialized verification consumer
        self.verification_consumer = None
        self.committed_offsets = {}

    # ------------------------------------------------------------------
    # SETUP: Préparer le consumer de vérification À L'AVANCE
    # ------------------------------------------------------------------
    def setup_verification_consumer(self):
        """
        Setup verification consumer BEFORE detecting restart.
        This way we're ready to read immediately when restart happens.
        """
        print("=" * 70)
        print("SETUP - Preparing verification consumer...")
        print("=" * 70)
        
        # Lire les offsets committés du consumer Redis
        print("Reading committed offsets from Redis consumer group...")
        temp_consumer = Consumer(
            {
                "bootstrap.servers": "kafka:9092",
                "group.id": "python-group",
                "enable.auto.commit": False,
            }
        )
        temp_consumer.subscribe(["test-topic"])
        
        assignment = None
        for i in range(20):
            temp_consumer.poll(0.5)
            assignment = temp_consumer.assignment()
            if assignment:
                print(f"✅ Got assignment after {(i+1)*0.5:.1f}s")
                break
        
        if not assignment:
            print("❌ ERROR: Could not get partition assignment")
            temp_consumer.close()
            return False
        
        self.committed_offsets = {}
        for partition in assignment:
            committed = temp_consumer.committed([partition], timeout=10.0)
            if committed and committed[0] and committed[0].offset >= 0:
                offset = committed[0].offset
                self.committed_offsets[partition.partition] = offset
                print(f"  Partition {partition.partition}: committed offset = {offset}")
            else:
                low, high = temp_consumer.get_watermark_offsets(partition, timeout=5.0)
                self.committed_offsets[partition.partition] = low
                print(f"  Partition {partition.partition}: no committed offset, using earliest = {low}")
        
        temp_consumer.close()
        
        # Créer le consumer de vérification
        print("\nCreating verification consumer...")
        unique_group_id = f"dummy-verify-{int(time.time() * 1000)}"
        self.verification_consumer = Consumer(
            {
                "bootstrap.servers": "kafka:9092",
                "group.id": unique_group_id,
                "enable.auto.commit": False,
            }
        )
        self.verification_consumer.subscribe(["test-topic"])
        
        # Attendre l'assignation
        assignment = None
        for i in range(20):
            self.verification_consumer.poll(0.5)
            assignment = self.verification_consumer.assignment()
            if assignment:
                print(f"✅ Got assignment after {(i+1)*0.5:.1f}s")
                break
        
        if not assignment:
            print("❌ ERROR: Could not get partition assignment")
            self.verification_consumer.close()
            self.verification_consumer = None
            return False
        
        # Se positionner aux offsets committés
        print("Seeking to committed offsets...")
        for partition in assignment:
            if partition.partition in self.committed_offsets:
                offset = self.committed_offsets[partition.partition]
                self.verification_consumer.seek(
                    TopicPartition(partition.topic, partition.partition, offset)
                )
                print(f"  Partition {partition.partition}: ready at offset {offset}")
        
        print("✅ Verification consumer ready!")
        print("=" * 70)
        return True

    # ------------------------------------------------------------------
    # MONITORING: détecter les restarts et retourner immédiatement
    # ------------------------------------------------------------------
    def wait_for_redis_consumer_restart(self):
        """
        Monitor Redis consumer and detect when it restarts.
        Returns immediately when restart is detected.
        """
        print("=" * 70)
        print("MONITORING MODE - Waiting for Redis Consumer Restart")
        print("=" * 70)
        print("Verification consumer is READY and waiting...")
        print("Checking every 0.1 second for immediate detection...")

        # Initialiser le compteur connu
        if self.last_known_restart_count is None:
            restart_count = self.redisclient.get(self.redis_consumer_restart_count_key)
            if restart_count:
                self.last_known_restart_count = int(restart_count)
            else:
                self.last_known_restart_count = 0
            print(f"Current restart count: {self.last_known_restart_count}")

        check_interval = 0.1  # Check every 100ms for faster detection
        checks_performed = 0

        try:
            while True:
                time.sleep(check_interval)
                checks_performed += 1

                current_restart_count = self.redisclient.get(
                    self.redis_consumer_restart_count_key
                )
                if current_restart_count:
                    current_restart_count = int(current_restart_count)

                    if current_restart_count > self.last_known_restart_count:
                        print("=" * 70)
                        print("🚨 REDIS CONSUMER RESTART DETECTED!")
                        print("=" * 70)
                        print(f"Previous count: {self.last_known_restart_count}")
                        print(f"Current count:  {current_restart_count}")
                        detection_time = datetime.now(timezone.utc)
                        print(f"Time: {detection_time.isoformat()}")
                        print("=" * 70)
                        self.last_known_restart_count = current_restart_count
                        return detection_time

                # Heartbeat info every 10 seconds
                if checks_performed % 100 == 0:  # Every 10s (100 * 0.1s)
                    last_heartbeat = self.redisclient.get(
                        self.redis_consumer_heartbeat_key
                    )
                    if last_heartbeat:
                        heartbeat_time = datetime.fromisoformat(last_heartbeat)
                        age = (datetime.now(timezone.utc) - heartbeat_time).total_seconds()
                        print(f"Consumer heartbeat: {age:.1f}s ago (count={self.last_known_restart_count})")

        except KeyboardInterrupt:
            print("Monitoring stopped by user")
            return None

    # ------------------------------------------------------------------
    # Attendre que le consumer soit prêt à lire Kafka
    # ------------------------------------------------------------------
    def wait_for_consumer_kafka_ready(self, timeout=5.0):
        """
        Wait for the Redis consumer to signal it's ready to read from Kafka.
        This ensures we start capturing at the same time as the consumer.
        """
        print("\n⏳ Waiting for consumer to be ready to read Kafka...")
        print(f"   Checking '{self.redis_consumer_kafka_ready_key}' flag (timeout: {timeout}s)...")
        
        start_time = time.time()
        checks = 0
        
        while (time.time() - start_time) < timeout:
            ready_flag = self.redisclient.get(self.redis_consumer_kafka_ready_key)
            if ready_flag:
                elapsed = time.time() - start_time
                print(f"✅ Consumer is READY! (detected after {elapsed:.3f}s)")
                return True
            
            time.sleep(0.05)  # Check every 50ms
            checks += 1
        
        print(f"⚠️  Timeout waiting for consumer ready flag ({timeout}s)")
        print("   Proceeding anyway, but timing might be slightly off...")
        return False

    # ------------------------------------------------------------------
    # VERIFICATION: Lire pendant 1 seconde dès maintenant
    # ------------------------------------------------------------------
    def verify_resynchronization(self, t0):
        """
        Read Kafka events for 1 second starting NOW (at t0).
        The verification consumer is already positioned at the right offset.
        """
        t1 = t0 + timedelta(seconds=0.5) 
        
        print("=" * 70)
        print("⚡ ZERO-LOSS VERIFICATION (IMMEDIATE)")
        print("=" * 70)
        print(f"[t0] Starting capture at: {t0.isoformat()}")
        print(f"[t1] Ending capture at:   {t1.isoformat()}")
        print(f"Reading from offsets: {self.committed_offsets}")
        print("=" * 70)

        # ------------------------------------------------------------------
        # Lire les événements pendant [t0, t1]
        # ------------------------------------------------------------------
        kafka_events = []
        events_read = 0
        
        print("📖 Reading Kafka events NOW...")
        
        while datetime.now(timezone.utc) < t1:
            remaining_time = (t1 - datetime.now(timezone.utc)).total_seconds()
            if remaining_time <= 0:
                break
                
            poll_timeout = min(0.05, max(0.01, remaining_time))
            msg = self.verification_consumer.poll(poll_timeout)
            
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    continue

            events_read += 1

            try:
                event = json.loads(msg.value().decode("utf-8"))
                kafka_events.append(event)
                
                if events_read % 10 == 0:
                    print(f"  📨 Captured {len(kafka_events)} events (offset {msg.offset()})...")

            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        print(f"✅ Captured {len(kafka_events)} events during 1-second window")
        
        if not kafka_events:
            print("⚠️  No events captured - consumer may have been caught up")
            return self.create_empty_report(t0, t1)

        # ------------------------------------------------------------------
        # Construire l'état Kafka
        # ------------------------------------------------------------------
        print("\n📊 Building Kafka reference state...")
        
        kafka_events.sort(
            key=lambda e: datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
        )
        
        kafka_latest_state = {}
        for event in kafka_events:
            resource_id = f"{event['resource_type']}:{event['resource_id']}"
            event_time = datetime.fromisoformat(
                event["timestamp"].replace("Z", "+00:00")
            )
            if resource_id not in kafka_latest_state:
                kafka_latest_state[resource_id] = event
            else:
                existing_time = datetime.fromisoformat(
                    kafka_latest_state[resource_id]["timestamp"].replace("Z", "+00:00")
                )
                if event_time >= existing_time:
                    kafka_latest_state[resource_id] = event

        print(f"✅ Kafka reference: {len(kafka_latest_state)} unique resources")

        # ------------------------------------------------------------------
        # Attendre puis capturer Redis
        # ------------------------------------------------------------------
        print("\n⏳ Waiting 2s for Redis consumer to process events...")
        time.sleep(2)

        print("📸 Capturing Redis state...")
        redis_capture_time = datetime.now(timezone.utc)
        cursor = 0
        redis_state = {}

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
                    if not data:
                        continue
                    resource_id = f"{data.get('resource_type')}:{data.get('resource_id')}"
                    redis_state[resource_id] = data

            if cursor == 0:
                break

        print(f"✅ Redis has {len(redis_state)} resources")

        # ------------------------------------------------------------------
        # Comparaison : Vérifier que Redis a traité TOUS les événements capturés
        # ------------------------------------------------------------------
        print("\n🔍 Comparing captured Kafka events vs Redis...")
        print(f"Goal: Ensure Redis processed all {len(kafka_latest_state)} resources from capture window")
        
        missing = []
        outdated = []
        correct = 0

        for resource_id, kafka_event in kafka_latest_state.items():
            kafka_event_id = kafka_event["event_id"]
            kafka_time = datetime.fromisoformat(
                kafka_event["timestamp"].replace("Z", "+00:00")
            )
            
            if resource_id not in redis_state:
                # Ressource pas dans Redis = le consumer n'a PAS traité cet événement
                print(f"  ❌ MISSING: {resource_id}")
                print(f"      Application captured: {kafka_event_id[:8]}... at {kafka_event['timestamp']}")
                print(f"      Redis: NOT FOUND")
                missing.append({
                    "resource_id": resource_id,
                    "kafka_event_id": kafka_event_id,
                    "kafka_timestamp": kafka_event["timestamp"],
                })
                continue

            redis_data = redis_state[resource_id]
            
            if "timestamp" not in redis_data:
                print(f"  ⚠️  OUTDATED (no timestamp): {resource_id}")
                outdated.append({
                    "resource_id": resource_id,
                    "kafka_event_id": kafka_event_id,
                    "redis_event_id": redis_data.get("event_id", "N/A"),
                    "reason": "missing_timestamp"
                })
                continue
            
            try:
                redis_time = datetime.fromisoformat(
                    redis_data["timestamp"].replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                print(f"  ⚠️  OUTDATED (invalid timestamp): {resource_id}")
                outdated.append({
                    "resource_id": resource_id,
                    "kafka_event_id": kafka_event_id,
                    "redis_event_id": redis_data.get("event_id", "N/A"),
                    "reason": "invalid_timestamp"
                })
                continue
            
            # LOGIQUE CLÉ : Si l'application a capturé un événement avec timestamp T,
            # Redis DOIT avoir un timestamp >= T (sinon il a loupé cet événement)
            if redis_time < kafka_time:
                # Redis a un timestamp PLUS ANCIEN = le consumer n'a PAS traité l'événement capturé
                timediff = (kafka_time - redis_time).total_seconds()
                print(f"  ⚠️  OUTDATED: {resource_id} (consumer is {timediff:.3f}s behind)")
                print(f"      Application captured: event at {kafka_event['timestamp']}")
                print(f"      Redis has:           event at {redis_data['timestamp']}")
                print(f"      → Consumer missed this event during resync!")
                outdated.append({
                    "resource_id": resource_id,
                    "kafka_event_id": kafka_event_id,
                    "redis_event_id": redis_data.get("event_id", "N/A"),
                    "kafka_timestamp": kafka_event["timestamp"],
                    "redis_timestamp": redis_data["timestamp"],
                    "timediff_seconds": timediff,
                })
            else:
                # Redis a un timestamp >= kafka_time = OK, le consumer a bien traité l'événement
                correct += 1
                if correct <= 3:  # Afficher seulement les 3 premiers
                    if redis_time == kafka_time:
                        print(f"  ✅ CORRECT: {resource_id} (exact match)")
                    else:
                        timediff = (redis_time - kafka_time).total_seconds()
                        print(f"  ✅ CORRECT: {resource_id} (Redis +{timediff:.3f}s newer - OK)")

        total_issues = len(missing) + len(outdated)
        
        print("\n" + "=" * 70)
        print("📊 VERIFICATION RESULTS")
        print("=" * 70)
        print(f"Captured window: [{t0.isoformat()}] → [{t1.isoformat()}]")
        print(f"Wait time for consumer: 2.0s")
        print("")
        print(f"✅ Correct:      {correct} / {len(kafka_latest_state)}")
        print(f"❌ Missing:      {len(missing)}")
        print(f"⚠️  Outdated:     {len(outdated)}")
        print("")
        
        if total_issues == 0:
            print("🎉 ZERO-LOSS VERIFIED!")
            print("   Redis consumer processed ALL captured events correctly.")
            print("   No events were missed during resynchronization.")
        else:
            print(f"⚠️  {total_issues} ISSUE(S) DETECTED:")
            if len(missing) > 0:
                print(f"   - {len(missing)} events NOT FOUND in Redis")
                print(f"     → Consumer did not process these events")
            if len(outdated) > 0:
                print(f"   - {len(outdated)} events OUTDATED in Redis")
                print(f"     → Consumer has older version, missed updates")

        # ------------------------------------------------------------------
        # Réparation
        # ------------------------------------------------------------------
        repaired = 0
        if total_issues > 0:
            print("\n🔧 Repairing Redis...")
            
            for item in missing + outdated:
                resource_id = item["resource_id"]
                kafka_event = kafka_latest_state.get(resource_id)
                if not kafka_event:
                    continue

                key = f"resource:{kafka_event['resource_type']}:{kafka_event['resource_id']}"
                try:
                    self.redisclient.hset(key, mapping=kafka_event)
                    repaired += 1
                except Exception as e:
                    print(f"  ❌ Error repairing {key}: {e}")
            
            print(f"✅ Repaired {repaired} resources")

        # ------------------------------------------------------------------
        # Rapport
        # ------------------------------------------------------------------
        verification_end_time = datetime.now(timezone.utc)
        report = {
            "verification_number": self.total_verifications_performed + 1,
            "timestamp": t0.isoformat(),
            "duration_seconds": (verification_end_time - t0).total_seconds(),
            "window": {
                "t0": t0.isoformat(),
                "t1": t1.isoformat(),
            },
            "committed_offsets": self.committed_offsets,
            "metrics": {
                "kafka_events_in_window": len(kafka_events),
                "unique_kafka_resources": len(kafka_latest_state),
                "redis_resources": len(redis_state),
                "correctly_synchronized": correct,
                "missing_in_redis": len(missing),
                "outdated_in_redis": len(outdated),
                "total_issues": total_issues,
                "repaired": repaired,
            },
            "issues": {
                "missing": missing,
                "outdated": outdated,
            },
            "status": "ZERO-LOSS" if total_issues == 0 else "ISSUES-DETECTED",
        }

        filename = f"tmp_verification_report_{self.total_verifications_performed + 1}.json"
        with open(filename, "w") as f:
            json.dump(report, f, indent=2)
        
        with open("tmp_verification_history.jsonl", "a") as f:
            f.write(json.dumps(report) + "\n")

        self.total_verifications_performed += 1
        print(f"\n📄 Report saved: {filename}")
        print("=" * 70)
        return report

    def create_empty_report(self, t0, t1):
        """Create report when no events captured"""
        report = {
            "verification_number": self.total_verifications_performed + 1,
            "timestamp": t0.isoformat(),
            "duration_seconds": 1.0,
            "window": {"t0": t0.isoformat(), "t1": t1.isoformat()},
            "committed_offsets": self.committed_offsets,
            "metrics": {
                "kafka_events_in_window": 0,
                "unique_kafka_resources": 0,
                "redis_resources": 0,
                "correctly_synchronized": 0,
                "missing_in_redis": 0,
                "outdated_in_redis": 0,
                "total_issues": 0,
                "repaired": 0,
            },
            "status": "NO-EVENTS",
        }
        
        filename = f"tmp_verification_report_{self.total_verifications_performed + 1}.json"
        with open(filename, "w") as f:
            json.dump(report, f, indent=2)
        
        self.total_verifications_performed += 1
        return report

    # ------------------------------------------------------------------
    # MAIN LOOP
    # ------------------------------------------------------------------
    def start(self):
        print("=" * 70)
        print("🚀 DUMMY APPLICATION - Zero-Loss Verification (IMMEDIATE MODE)")
        print("=" * 70)
        print("Strategy: Prepare consumer in advance, detect restart instantly")
        print("=" * 70)

        try:
            while True:
                # 1) Setup verification consumer AVANT de détecter le restart
                if not self.setup_verification_consumer():
                    print("❌ Failed to setup verification consumer, retrying in 5s...")
                    time.sleep(5)
                    continue

                # 2) Attendre le restart (consumer déjà prêt)
                restart_time = self.wait_for_redis_consumer_restart()
                if not restart_time:
                    break

                # 3) NOUVEAU : Attendre que le consumer soit prêt à lire Kafka
                self.wait_for_consumer_kafka_ready(timeout=5.0)
                
                # 4) MAINTENANT définir t0 (le consumer est prêt)
                t0 = datetime.now(timezone.utc)
                print(f"⚡ Starting verification NOW at {t0.isoformat()}")
                
                # 5) Lancer la vérification
                self.verify_resynchronization(t0)

                # 6) Fermer le consumer de vérification
                if self.verification_consumer:
                    self.verification_consumer.close()
                    self.verification_consumer = None

                print("\n✅ Verification complete - Preparing for next restart...")
                print(f"Total verifications: {self.total_verifications_performed}\n")

        except KeyboardInterrupt:
            print("\n🛑 Application stopped")
            if self.verification_consumer:
                self.verification_consumer.close()
            
            print("=" * 70)
            print("📊 SUMMARY")
            print("=" * 70)
            print(f"Total verifications: {self.total_verifications_performed}")
            if self.verification_reports:
                for i, report in enumerate(self.verification_reports, 1):
                    print(f"  {i}. {report['status']} - {report['metrics']['total_issues']} issues")
            print("=" * 70)


if __name__ == "__main__":
    app = DummyApplication()
    app.start()
