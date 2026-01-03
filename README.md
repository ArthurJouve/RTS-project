# RTS Project – Kafka, Python & Grafana (Docker)

## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/ArthurJouve/RTS-project.git
cd RTS-project
```

If you want to use the stable configuration (including Docker, Kafka, Redis and Grafana), make sure you are on the **main** branch:

```bash
git checkout main
```

---

### 2. Install Docker and Docker Compose

If Docker is not installed on your system, follow the official installation guide for your OS:

- **Linux (Ubuntu/Debian)**: https://docs.docker.com/engine/install/ubuntu/
- **macOS**: https://docs.docker.com/desktop/install/mac-install/
- **Windows**: https://docs.docker.com/desktop/install/windows-install/

Verify the installation:

```bash
docker --version
docker compose version
```

On Linux, if you get permission errors:

```bash
sudo usermod -aG docker $USER
newgrp docker
```

---

### 3. Build and start the containers

If you already ran the project before, it is recommended to stop everything first:

```bash
docker compose down
```

Then build and start all services:

```bash
docker compose up -d --build
```

This will start the following services:

- **Zookeeper** – Kafka coordination  
- **Kafka** – message broker  
- **Redis** – state storage  
- **Python app** – producer, consumer and verification logic  
- **JSON server** – exposes verification results via HTTP  
- **Grafana** – visualization dashboard  

Check that everything is running:

```bash
docker ps
```

---

### 4. Create the Kafka topic manually (if needed)

Although Kafka is configured with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, automatic topic creation may fail in some environments.

To ensure the topic exists:

```bash
docker exec -it kafka bash
```

```bash
kafka-topics --create   --topic test-topic   --bootstrap-server localhost:9092   --replication-factor 1   --partitions 1
```

Verify:

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

### 5. Run the Producer, Consumer and Application

All Python components run inside the same container.

**Consumer**
```bash
docker exec -it rts-project-python-app python consumer.py
```

**Producer (new terminal)**
```bash
docker exec -it rts-project-python-app python producer.py
```

**Application (new terminal)**
```bash
docker exec -it rts-project-python-app python application.py
```

The producer generates fake network reports.  
The consumer and application compare Redis and application state.

---

### 6. Verification results

Each time the application runs:

- A **current verification report** is generated  
  `current_verification_report.json`

- A **history of all verifications** is appended to  
  `verification_history.jsonl`

⚠️ These files are runtime data and are not versioned in Git.

---

## 7. Grafana – Monitoring & Dashboards

### 7.1 Access Grafana

Once Docker is running, open your browser:

```
http://localhost:3000
```

Default credentials:

- **User:** admin  
- **Password:** admin  

(You will be asked to change the password on first login.)

---

### 7.2 Automatic dashboard provisioning

Grafana is fully configured automatically via Docker:

- JSON API datasource
- RTS Verification dashboard
- Panels for:
  - Last verification status
  - Total issues over time
  - Verification duration
  - Synchronization metrics

👉 **No manual dashboard import is required.**

On first startup, the dashboard may show **“No data”** until:

- the producer, consumer and application run
- verification files are generated

Once data exists, panels update automatically.

---

### 7.3 Checking the JSON API datasource UID (important)

Dashboards reference the datasource by **UID**.

To check it in Grafana:

1. Open Grafana  
2. Go to **Connections → Data sources**  
3. Click on **JSON API**
4. Look at the URL, it will look like:

```
http://localhost:3000/connections/datasources/edit/PD5070BC1AA9F8304
```

➡️ The last part is the **datasource UID**

If dashboards show **“No data”**, make sure:

- this UID matches the one used in the dashboard JSON
- the JSON server is running on port **5001**

---

### 7.4 JSON Server endpoints

The JSON server exposes verification data to Grafana:

**Latest verification**
```
http://localhost:5001/latest
```

**Verification history**
```
http://localhost:5001/history
```

You can test them manually with:

```bash
curl http://localhost:5001/latest
curl http://localhost:5001/history
```

---

### 8. Stop the environment

To stop all containers:

```bash
docker compose down
```

To fully clean images and volumes:

```bash
docker system prune -af
```

---
