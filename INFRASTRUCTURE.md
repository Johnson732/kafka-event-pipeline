# Kafka Infrastructure Setup

This document covers the full infrastructure setup for the Kafka Event Pipeline project — from GCP VM creation to running Kafka brokers with Docker.

---

## Architecture Overview

```
Your Local Machine (Spring Boot App)
        │
        ├── port 9092 ──→ kafka-1 (Broker 1)
        └── port 9093 ──→ kafka-2 (Broker 2)
                │               │
                └───────────────┘
                  INTERNAL network
                  (kafka-1:29092 ↔ kafka-2:29093)
                        │
                   zookeeper:2181
                   (coordinates both brokers)
```

---

## Step 1 — Create a GCP VM

### Go to GCP Console
1. Visit [console.cloud.google.com](https://console.cloud.google.com)
2. Navigate to **Compute Engine → VM Instances → Create Instance**

### VM Configuration
| Field | Value |
|-------|-------|
| Name | kafka-vm |
| Region | us-central1 |
| Zone | us-central1-a |
| Machine type | e2-medium (2 vCPU, 4GB RAM) |
| Boot disk | Ubuntu 22.04 LTS |
| Boot disk size | 20 GB |
| Firewall | Allow HTTP, Allow HTTPS |

Click **Create** and note down the **External IP** assigned to your VM.

---

## Step 2 — Create Firewall Rule

Kafka brokers and Zookeeper need specific ports open for external access.

### Go to VPC Network → Firewall → Create Rule

| Field | Value |
|-------|-------|
| Name | kafka-ports |
| Direction | Ingress |
| Action | Allow |
| Targets | All instances in the network |
| Source IPv4 ranges | 0.0.0.0/0 |
| Protocols and ports | TCP: 9092, 9093, 2181 |

Click **Create**.

> **Why these ports:**
> - `9092` → kafka-1 external listener (your app connects here)
> - `9093` → kafka-2 external listener (your app connects here)
> - `2181` → Zookeeper (broker coordination)

> **Note:** If your VM has no network tag, set Targets to "All instances in the network" so the rule applies to all VMs including kafka-vm.

---

## Step 3 — Install Docker on the VM

SSH into the VM via the GCP Console SSH button, then run:

```bash
# Update package list
sudo apt-get update

# Install Docker and Docker Compose
sudo apt-get install -y docker.io docker-compose

# Add your user to docker group (avoid sudo on every command)
sudo usermod -aG docker $USER
newgrp docker

# Verify installation
docker --version
```

Expected output:
```
Docker version 26.1.5+dfsg1, build a72d7cd
```

---

## Step 4 — Create docker-compose.yml on the VM

```bash
# Create a directory for Kafka
mkdir kafka && cd kafka

# Create the compose file
nano docker-compose.yml
```

Paste the following content:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka-1:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:29092,EXTERNAL://<YOUR_VM_EXTERNAL_IP>:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_DEFAULT_REPLICATION_FACTOR: 2
      KAFKA_NUM_PARTITIONS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-2:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29093,EXTERNAL://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-2:29093,EXTERNAL://<YOUR_VM_EXTERNAL_IP>:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_DEFAULT_REPLICATION_FACTOR: 2
      KAFKA_NUM_PARTITIONS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Save with `Ctrl+X` → `Y` → `Enter`.

> Replace `<YOUR_VM_EXTERNAL_IP>` with your actual VM external IP (e.g. `34.47.193.96`).

---

## docker-compose.yml Explained

### Zookeeper
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.5.0
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181
```
Zookeeper is the **coordinator** of the Kafka cluster. It tracks which brokers are alive, who is the partition leader, and stores consumer group offset metadata. Port `2181` is what brokers use to register themselves and communicate with Zookeeper.

### Kafka Brokers (kafka-1 and kafka-2)

| Config | Purpose |
|--------|---------|
| `KAFKA_BROKER_ID` | Unique ID for each broker in the cluster (1 and 2) |
| `KAFKA_ZOOKEEPER_CONNECT` | Tells each broker where Zookeeper is |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Defines two listener types: INTERNAL and EXTERNAL |
| `KAFKA_LISTENERS` | Binds listeners to ports inside the container |
| `KAFKA_ADVERTISED_LISTENERS` | Addresses brokers tell clients to connect to |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | Brokers talk to each other via INTERNAL listener |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | Each partition is copied to 2 brokers for fault tolerance |
| `KAFKA_NUM_PARTITIONS` | Default number of partitions for new topics |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | Replication for `__consumer_offsets` internal topic |

### Why Two Listeners?

This is the key config that makes external access work:

```
INTERNAL → kafka-1:29092  (broker-to-broker communication inside Docker network)
EXTERNAL → 34.47.193.96:9092  (your Spring Boot app connecting from outside)
```

Without this split, both brokers advertise `localhost` which only works inside the container — other brokers and your app can't reach them.

---

## Step 5 — Kafka Commands

### Start Kafka
```bash
cd kafka
sudo docker-compose up -d
```

### Stop Kafka
```bash
sudo docker-compose down
```

### Stop and wipe all data (clean reset)
```bash
sudo docker-compose down -v
```
The `-v` flag removes Docker volumes, which clears all Kafka topic data and Zookeeper state. Use this when topics are in a broken state.

### Check running containers
```bash
sudo docker ps
```

### Check broker logs
```bash
# kafka-1 logs
sudo docker logs kafka-kafka-1-1 --tail 50

# kafka-2 logs
sudo docker logs kafka-kafka-2-1 --tail 50
```

### Restart Kafka
```bash
sudo docker-compose down
sudo docker-compose up -d
```

---

## Step 6 — Verify Connection from Local Machine

After Kafka is running on the VM, verify ports are reachable from your Mac:

```bash
# Test kafka-1
nc -zv <YOUR_VM_EXTERNAL_IP> 9092

# Test kafka-2
nc -zv <YOUR_VM_EXTERNAL_IP> 9093
```

Expected output:
```
Connection to 34.47.193.96 port 9092 [tcp/XmlIpcRegSvc] succeeded!
Connection to 34.47.193.96 port 9093 [tcp/XmlIpcRegSvc] succeeded!
```

---

## Common Issues and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `INVALID_REPLICATION_FACTOR` | Topic created with replication > broker count | `docker-compose down -v && docker-compose up -d` |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic in broken state from previous failed creation | Same as above — full clean restart with `-v` |
| `Connection timed out` on nc | Firewall rule not applied to VM | Set firewall target to "All instances in network" |
| `__consumer_offsets` replication error | Default replication factor is 3, only 2 brokers | Add `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` |
| VM gets new IP after restart | GCP assigns dynamic external IPs by default | Update `KAFKA_ADVERTISED_LISTENERS` and `application.yml` with new IP, or reserve a static IP in GCP |

---

## Important Note on VM External IP

GCP assigns a **dynamic external IP** by default — it changes every time the VM restarts. To avoid updating configs every time, reserve a **static IP**:

Go to **VPC Network → IP Addresses → Reserve External Static Address** and attach it to `kafka-vm`.