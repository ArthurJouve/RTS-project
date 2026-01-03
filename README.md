
# RTS Project - Kafka + Python (via Docker)

## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/ArthurJouve/RTS-project.git
cd RTS-project
````

If you want to use this configuration and following steps (including Docker setup and topic creation), be sure to be into the `main` branch:

```bash
git checkout main
```

---

### 2. Install Docker and Docker Compose

If Docker is not installed on your system, follow the official installation guide for your OS:

* **Linux (Ubuntu/Debian)**: [https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/)
* **macOS**: [https://docs.docker.com/desktop/install/mac-install/](https://docs.docker.com/desktop/install/mac-install/)
* **Windows**: [https://docs.docker.com/desktop/install/windows-install/](https://docs.docker.com/desktop/install/windows-install/)

After installation, verify that Docker is working:

```bash
docker --version
docker compose version
```

If you get a permission error when using Docker on Linux, run:

```bash
sudo usermod -aG docker $USER
newgrp docker
```

---

### 3. Build and start the containers
If you are running the program for a second time or more, maybe it will be needed to run it before: 

```bash
docker compose down
```

Once Docker is ready (after compose down if used), build and start all the project containers using:

```bash
docker compose up -d --build
```

This will start three services:

* **Zookeeper** (cluster coordination)
* **Kafka** (message broker)
* **Python app** (runs the producer and consumer)

You can check that everything is running with:

```bash
docker ps
```

---

### 4. Create the Kafka topic manually

Even though Kafka is configured with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, automatic topic creation may fail in some environments.

To ensure the topic exists, manually create it inside the Kafka container:

1. Open a terminal inside the Kafka container:

   ```bash
   docker exec -it kafka bash
   ```

2. Run the topic creation command:

   ```bash
   kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 \
   --replication-factor 1 --partitions 1
   ```

3. Confirm that the topic was created successfully:

   ```bash
   kafka-topics --list --bootstrap-server localhost:9092
   ```

You should see:

```
test-topic
```

Exit the container:

```bash
exit
```

---

### 5. Run the Producer / Consumer / Application

Now that Kafka is ready, you can start both Python scripts.

#### Run the Consumer:

```bash
docker exec -it rts-project-python-app python consumer.py
```

#### Run the Producer (in another terminal):

```bash
docker exec -it rts-project-python-app python producer.py
```

Messages will be automatically sent by the producer, using fake reports of a virtual network. 

#### Run the Application (in another terminal):

```bash
docker exec -it rts-project-python-app python application.py
```
---

### 6. Results 

Each time the application restart the consumer, it produces a report of the comparison between Redis and the save of the application (more details in the project report).
The file "current_verification_report.json" is created, with the current verification. 
The whole history of all verifications is also created and updated each time in "verification_history.jsonl" file.

---

### 7. Setup the Grafana visualisation

---

### 8. Stop the environment

When you’re done, stop and remove all containers with:

```bash
docker compose down
```

If you want to clean up old images and volumes completely:

```bash
docker system prune -af
```
