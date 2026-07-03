# Kafka Event Pipeline — Issues, Errors & Fixes

This document covers every error, issue, and failure we hit during the project — in the exact order they occurred — with the root cause and how we fixed it.

---

## Error 1 — Java Version Mismatch

### Error
```
Fatal error compiling: error: release version 21 not supported
```

### When
First time running `./mvnw spring-boot:run` after creating the project.

### Root Cause
Two different Java versions were installed:
- `java -version` → Java 17 (used by `./mvnw`)
- `mvn -version` → Java 25 (used by `mvn` command)

The Maven wrapper `./mvnw` picked up Java 17 but `pom.xml` declared `java.version=21`. Java 17 cannot compile code targeting Java 21.

### Fix
Changed `pom.xml` to target Java 17:
```xml
<properties>
    <java.version>17</java.version>
</properties>
```

### Lesson
Always verify which Java version Maven is actually using with `mvn -version`, not just `java -version`. They can differ on machines with multiple JDK installations.

---

## Error 2 — Jackson ClassNotFoundException

### Error
```
org.apache.kafka.common.KafkaException: Failed to construct kafka producer
Caused by: java.lang.ClassNotFoundException: com.fasterxml.jackson.databind.JavaType
```

### When
First curl request to `/api/orders/test` after fixing Error 1.

### Root Cause
`JsonSerializer` (used as the Kafka value serializer) depends on Jackson Databind at runtime, but it was not on the classpath. Spring Boot 4.x uses Jackson 3.x (`tools.jackson`) internally, but the Kafka client's `JsonSerializer` looks for Jackson 2.x (`com.fasterxml.jackson`).

### Fix
Added `jackson-databind` to `pom.xml`:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
```

### Lesson
Spring Boot 4.x ships with Jackson 3.x but Kafka's built-in serializers still depend on Jackson 2.x. These are different packages and don't share classes.

---

## Error 3 — Instant Serialization Failure

### Error
```
SerializationException: Can't serialize data [OrderEvent[...]]
Caused by: InvalidDefinitionException: Java 8 date/time type `java.time.Instant`
not supported by default: add Module "com.fasterxml.jackson.datatype:jackson-datatype-jsr310"
```

### When
After fixing Error 2, next curl request.

### Root Cause
`OrderEvent` had a field `Instant timestamp`. Jackson doesn't serialize `java.time.Instant` by default — it needs the JSR310 module registered.

### Fix (Attempt 1)
Added `jackson-datatype-jsr310` to `pom.xml` and added to `application.yml`:
```yaml
spring:
  jackson:
    serialization:
      write-dates-as-timestamps: false
```

This caused Error 4.

### Final Fix
Changed `timestamp` field in `OrderEvent` from `Instant` to `String`:
```java
public record OrderEvent(
    ...
    String timestamp   // was: Instant timestamp
) {
    public static OrderEvent create(...) {
        return new OrderEvent(
            ...
            Instant.now().toString()   // convert to String at creation
        );
    }
}
```

### Lesson
Spring Boot 4.x uses Jackson 3.x which has different package names (`tools.jackson` instead of `com.fasterxml.jackson`). JSR310 module configuration behaves differently. Simplest fix was to store timestamp as String and avoid the conflict entirely.

---

## Error 4 — Jackson SerializationFeature Enum Not Found

### Error
```
Failed to bind properties under 'spring.jackson.serialization' to
java.util.Map<tools.jackson.databind.SerializationFeature, java.lang.Boolean>
Reason: failed to convert java.lang.String to tools.jackson.databind.SerializationFeature
(caused by: No enum constant tools.jackson.databind.SerializationFeature.write-dates-as-timestamps)
```

### When
After adding `spring.jackson.serialization.write-dates-as-timestamps: false` to fix Error 3.

### Root Cause
Spring Boot 4.x uses Jackson 3.x which moved to `tools.jackson` package. The `SerializationFeature` enum no longer lives at `com.fasterxml.jackson.databind.SerializationFeature`. The `application.yml` property binding failed because the enum class path changed.

### Fix
Removed `spring.jackson` config entirely from `application.yml` and changed `timestamp` to `String` in `OrderEvent` (see Error 3 final fix).

### Lesson
When upgrading to Spring Boot 4.x, Jackson 3.x compatibility issues affect many things beyond just serializers. Avoiding Jackson-specific types in your Kafka messages is the safest approach.

---

## Error 5 — JsonSerializer Still Looking for Jackson 2.x

### Error
```
Failed to construct kafka producer
Caused by: java.lang.ClassNotFoundException: com.fasterxml.jackson.core.type.TypeReference
```

### When
After fixing the Instant issue, still using `JsonSerializer` as value-serializer.

### Root Cause
Even with `jackson-databind` on classpath, Kafka's `JsonSerializer` internally looks for `com.fasterxml.jackson.core.type.TypeReference` which is a Jackson 2.x class. Spring Boot 4.x ships Jackson 3.x which uses `tools.jackson.core.type.TypeReference`. The class names are different, so Kafka's serializer can't find it.

### Fix
Switched from `JsonSerializer` to `StringSerializer` for both key and value, and manually built the JSON string in the producer:

```yaml
producer:
  key-serializer: org.apache.kafka.common.serialization.StringSerializer
  value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

```java
String payload = String.format(
    "{\"orderId\":\"%s\",\"userId\":\"%s\",...}",
    event.orderId(), event.userId(), ...
);
kafkaTemplate.send(orderTopic, event.orderId(), payload);
```

Also changed `KafkaTemplate<String, OrderEvent>` to `KafkaTemplate<String, String>`.

### Lesson
Spring Boot 4.x + Spring Kafka 4.x + Kafka Clients 4.x is a very new combination. Jackson 2.x vs 3.x is the biggest compatibility pain point. Using `StringSerializer` with manual JSON avoids the entire problem.

---

## Error 6 — INVALID_REPLICATION_FACTOR (Local Docker)

### Error
```
WARN: The metadata response from the cluster reported a recoverable issue:
{order-events=INVALID_REPLICATION_FACTOR}
```

### When
After the producer started working, topics were getting created with broken state.

### Root Cause
We set `KAFKA_NUM_PARTITIONS: 6` in docker-compose but only had 2 brokers. Kafka tried to create topics with 6 partitions and replicate across more brokers than existed. Topics ended up in a partially created broken state.

### Fix (Attempt 1)
Reduced partitions to 2 in `KafkaProducerConfig.java` and did a full clean restart:
```bash
docker-compose down -v
docker-compose up -d
```

The `-v` flag wipes Docker volumes, clearing all broken topic data from Zookeeper.

### Fix (Attempt 2)
Removed `KAFKA_NUM_PARTITIONS: 6` from docker-compose entirely to stop Kafka from overriding Spring's topic creation.

### Lesson
`KAFKA_NUM_PARTITIONS` in docker-compose sets the broker-level default but can conflict with application-level topic creation. Always match partition count and replication factor to your actual broker count.

---

## Error 7 — UNKNOWN_TOPIC_OR_PARTITION (Retrying Forever)

### Error
```
Got error produce response on topic-partition order-events-5, retrying (2147483646 attempts left).
Error: UNKNOWN_TOPIC_OR_PARTITION
```

### When
After reducing partitions to 2, messages to partition=5 kept failing because the topic was created in a broken state earlier (with 6 partitions) and the old metadata was cached.

### Root Cause
Kafka had cached the old broken topic metadata (6 partitions) and kept trying to send to partition=5 which didn't exist. Even after restart, Zookeeper retained the old broken topic data because we didn't wipe volumes.

### Fix
Full clean restart with volume wipe:
```bash
docker-compose down -v
docker-compose up -d
```

Tried to delete topics manually first but got timeout errors because the docker-compose internal network uses container names not `localhost`:
```bash
# This failed:
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic order-events

# This also failed (container not part of same network context):
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --delete --topic order-events
```

Volume wipe was the cleanest fix.

### Lesson
`docker-compose down` alone does not remove volumes. Always use `-v` when you need a truly clean Kafka state. Kafka topic metadata persists in Zookeeper volumes across restarts.

---

## Error 8 — Docker Daemon Not Running

### Error
```
Cannot connect to the Docker daemon at unix:///Users/sribharath/.docker/run/docker.sock.
Is the docker daemon running?
```

### When
First time running `docker-compose up -d` on local Mac.

### Root Cause
Docker Desktop was installed but not running. Docker requires the daemon to be active before any CLI commands work.

### Fix
Opened Docker Desktop from Applications, waited for the whale icon in the Mac menu bar to stop animating (fully started), then reran the command.

### Lesson
On Mac, Docker runs as a desktop app. It doesn't start automatically unless configured to launch at login.

---

## Error 9 — Brokers Can't Communicate (Two Broker Setup)

### Error
```
Consumer disconnecting from node -2 due to socket connection setup timeout.
Bootstrap broker 34.14.207.7:9093 disconnected
```

### When
After deploying to GCP with 2 brokers, consumer could connect initially but then timed out trying to reach the second broker.

### Root Cause
Both brokers were advertising `localhost` as their address. When the consumer connected to kafka-1 and got told "kafka-2 is at localhost:9093", it tried to connect to its own localhost — not the GCP VM. Broker-to-broker communication also failed because containers can't reach each other via `localhost`.

### Fix
Added split listeners to docker-compose — INTERNAL for broker-to-broker (using Docker container names), EXTERNAL for app connections (using GCP external IP):

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:29092,EXTERNAL://34.47.193.96:9092
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

### Lesson
In a multi-broker Docker setup, `localhost` means different things inside vs outside the container. Always use container names for internal communication and external IP for outside access.

---

## Error 10 — GCP Firewall Ports Blocked

### Error
```
nc -zv 34.14.207.7 9092
nc: connectx to 34.14.207.7 port 9092 (tcp) failed: Operation timed out
```

### When
After deploying Kafka to GCP VM, local Spring Boot app couldn't reach the brokers.

### Root Cause
We created a firewall rule with **target tags**, but the VM had **no network tags** assigned. GCP firewall rules with target tags only apply to VMs that have that tag. Without the tag on the VM, the rule never applied and all ports remained blocked.

### Fix (Option 1 — used)
Changed the firewall rule target from "Specified target tags" to **"All instances in the network"**.

### Fix (Option 2)
Add the matching network tag to the VM under Compute Engine → VM → Edit → Network tags.

### Lesson
GCP firewall rules with target tags are silent — they don't warn you if no VMs have the tag. Always verify the rule is actually applying by testing with `nc -zv <ip> <port>` from outside.

---

## Error 11 — VM External IP Changed

### Symptom
App stopped connecting to Kafka after GCP VM was restarted. `nc -zv` to old IP timed out.

### Root Cause
GCP assigns **dynamic external IPs** by default. Every time the VM restarts, it gets a new IP. Our `KAFKA_ADVERTISED_LISTENERS` and `application.yml` had the old IP hardcoded.

### Fix
Checked new IP in GCP Console → updated `KAFKA_ADVERTISED_LISTENERS` in docker-compose on the VM → updated `bootstrap-servers` in local `application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: 34.47.193.96:9092,34.47.193.96:9093
```

Restarted docker-compose and the Spring Boot app.

### Permanent Fix (recommended)
Reserve a static external IP in GCP:
**VPC Network → IP Addresses → Reserve External Static Address → attach to kafka-vm**

### Lesson
Never hardcode dynamic IPs. In production, use DNS names. For local dev/learning on GCP, reserve a static IP to avoid this.

---

## Error 12 — __consumer_offsets Replication Factor Error

### Error
```
InvalidReplicationFactorException: Replication factor: 3 larger than available brokers: 2
Topic: __consumer_offsets
```

### When
Consumer was subscribed but never received any messages. Found in broker logs via `sudo docker logs kafka-kafka-1-1`.

### Root Cause
`__consumer_offsets` is Kafka's internal topic that stores consumer group offset commits. By default, Kafka tries to create it with `replication.factor=3` (production default). But we only had 2 brokers, so creation failed repeatedly. Without `__consumer_offsets`, consumer groups can't commit or read offsets — which is why the consumer was subscribed but never got partition assignments.

### Fix
Added to both brokers in docker-compose:
```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Then full clean restart:
```bash
sudo docker-compose down -v
sudo docker-compose up -d
```

### Lesson
Kafka's internal topics have their own replication factor settings separate from your application topics. On a 2-broker cluster, always explicitly set `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` (or 2 at most). Without this, consumer groups silently fail to work.

---

## Error 13 — Consumer Subscribed But Never Received Messages

### Symptom
```
Subscribed to topic(s): order-events   ← shown in logs
```
But no `Consumed →` log lines ever appeared, even after firing curl multiple times.

### Root Cause
This was caused by Error 12 (`__consumer_offsets` failing to create). Without the internal offset topic, Kafka couldn't complete consumer group coordination so no partition assignments were made — even though the subscribe call succeeded.

It looked like:
1. Consumer subscribes ✅
2. Kafka tries to assign partitions ❌ (needs `__consumer_offsets` to track the group)
3. Consumer polls forever but gets nothing

### Fix
Same as Error 12 — adding `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` and clean restart fixed both issues together.

### Lesson
`Subscribed to topic` does not mean partitions are assigned. Watch for `partitions assigned: [order-events-0, order-events-1]` in the logs to confirm the consumer is actually ready to receive.

---

## Error 14 — grep on Logs Showed Nothing

### Symptom
```bash
./mvnw spring-boot:run | grep "Order sent"
# blank output — nothing printed
```

### Root Cause
Spring Boot writes logs to **stderr**, not stdout. The pipe `|` only captures stdout. So `grep` received nothing.

### Fix
Redirect stderr to stdout before piping:
```bash
./mvnw spring-boot:run 2>&1 | grep "Order sent"
```

Or simply use IntelliJ's Run tab where all output (stdout + stderr) is shown together in a scrollable console.

### Lesson
Java application logs go to stderr by default. Always use `2>&1` when piping Spring Boot output, or use an IDE console for easier reading.

---

## Summary Table

| # | Error | Root Cause | Fix |
|---|-------|-----------|-----|
| 1 | Java version mismatch | `./mvnw` used Java 17, pom.xml said Java 21 | Changed pom.xml to Java 17 |
| 2 | Jackson ClassNotFoundException | Jackson 2.x not on classpath | Added `jackson-databind` dependency |
| 3 | Instant serialization failed | Jackson needs JSR310 module for `java.time.Instant` | Changed timestamp to String |
| 4 | SerializationFeature enum not found | Spring Boot 4.x uses Jackson 3.x with different package names | Removed spring.jackson config |
| 5 | TypeReference ClassNotFoundException | Kafka JsonSerializer needs Jackson 2.x, Spring Boot 4.x has Jackson 3.x | Switched to StringSerializer |
| 6 | INVALID_REPLICATION_FACTOR | 6 partitions requested but only 2 brokers | Reduced to 2 partitions, wiped volumes |
| 7 | UNKNOWN_TOPIC_OR_PARTITION retrying | Broken topic metadata cached in Zookeeper | `docker-compose down -v` |
| 8 | Docker daemon not running | Docker Desktop not started on Mac | Opened Docker Desktop |
| 9 | Broker-to-broker timeout | Both brokers advertised `localhost` | Added INTERNAL/EXTERNAL split listeners |
| 10 | GCP ports blocked | Firewall rule had target tags, VM had no tags | Changed to all instances target |
| 11 | VM IP changed after restart | GCP uses dynamic IPs by default | Updated IP in config, recommended static IP |
| 12 | `__consumer_offsets` replication error | Default replication factor 3 > 2 brokers | Added `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` |
| 13 | Consumer subscribed but no messages | `__consumer_offsets` creation failing silently | Same fix as Error 12 |
| 14 | grep showed nothing on logs | Spring Boot logs go to stderr not stdout | Used `2>&1` or IntelliJ console |