# Kafka Event Pipeline — Project Journey

This document covers everything we built and proved hands-on, in the exact order we did it.

---

## Project Goal

Build a real-time order event pipeline using Apache Kafka and Spring Boot that handles high throughput, demonstrates core Kafka concepts, and is tested against a real multi-broker cluster on GCP.

---

## Tech Stack

- Java 17
- Spring Boot 4.1.0
- Spring Kafka 4.1.0
- Apache Kafka 4.2.1 (2 brokers on GCP VM)
- Docker + Docker Compose
- Maven

---

## Step 1 — Spring Boot Project Setup

Created a new Spring Boot project from [start.spring.io](https://start.spring.io) with these dependencies:

| Dependency | Purpose |
|------------|---------|
| Spring for Apache Kafka | Core Kafka producer/consumer support |
| Spring Boot Actuator | Expose metrics and health endpoints |
| Spring Web | REST API to trigger events |

Changed `java.version` in `pom.xml` from 21 to 17 because the local JDK was Java 17.

Added extra dependencies manually:
- `jackson-databind` — JSON serialization for Kafka messages
- `jackson-datatype-jsr310` — Java date/time support (later replaced with String timestamp to avoid Jackson 3.x conflicts with Spring Boot 4.x)

---

## Step 2 — application.yml

Replaced `application.properties` with `application.yml`.

```yaml
spring:
  kafka:
    bootstrap-servers: 34.47.193.96:9092,34.47.193.96:9093
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      group-id: order-processor-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false
    listener:
      ack-mode: manual

server:
  port: 8080

topic:
  order: order-events
  payment: payment-events
  inventory: inventory-events
```

Key decisions:
- `bootstrap-servers` points to GCP VM where Kafka runs
- Both serializer and deserializer use String (not JSON) to avoid Jackson version conflicts
- `enable-auto-commit: false` and `ack-mode: manual` for manual offset control (added later)

---

## Step 3 — OrderEvent Model

Created `OrderEvent.java` as a Java **record** (Java 16+ feature).

```
orderId     → UUID, unique per order, used as Kafka message KEY
userId      → who placed the order
productId   → which product
quantity    → how many
totalAmount → order value
status      → ORDER_CREATED, PAYMENT_PROCESSED, etc.
timestamp   → when the event was created (stored as String)
```

Used a static factory method `OrderEvent.create(...)` so callers don't construct the object manually.

**Why record?** Eliminates boilerplate — no need to write constructor, getters, equals, hashCode manually. Perfect for immutable event objects.

---

## Step 4 — KafkaProducerConfig

Created `KafkaProducerConfig.java` in `config` package.

### Topics Created

| Topic | Partitions | Replicas |
|-------|-----------|---------|
| order-events | 2 | 1 |
| payment-events | 2 | 1 |
| inventory-events | 2 | 1 |

**Why 2 partitions?** We have 2 Kafka brokers on GCP. Each partition leader sits on a different broker. 2 partitions = 2 consumers can work in parallel on the same topic.

**Why replicas(1)?** We started with replicas(2) but hit `INVALID_REPLICATION_FACTOR` errors because `__consumer_offsets` (Kafka's internal topic) tried to create with replication factor 3 by default, but only 2 brokers were available. Fixed by adding `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` to docker-compose.

`NewTopic` beans tell Spring Kafka to auto-create topics on startup if they don't already exist.

`KafkaTemplate<String, String>` is the main class used to publish messages to Kafka.

---

## Step 5 — OrderEventProducer

Created `OrderEventProducer.java` in `producer` package.

### How publishing works

```java
kafkaTemplate.send(topic, key, value)
```

| Parameter | Value | Purpose |
|-----------|-------|---------|
| topic | order-events | which topic to publish to |
| key | event.orderId() | decides which partition the message lands on |
| value | JSON string | the actual message payload |

### Key → Partition Routing

Kafka uses this formula internally:
```
partition = hash(key) % number_of_partitions
```

So:
- Same `orderId` → always same partition → ordering preserved per order
- Different `orderId` → distributed across partitions → parallelism

### Async Sending

`kafkaTemplate.send()` returns a `CompletableFuture`. The app doesn't wait for Kafka to confirm — it moves on and the callback fires when Kafka responds.

```
Success callback → logs topic + partition + offset
Failure callback → logs error with orderId
```

### What we saw in logs

```
Order sent → topic=order-events, partition=0, offset=0, orderId=abc...
Order sent → topic=order-events, partition=1, offset=0, orderId=xyz...
Order sent → topic=order-events, partition=0, offset=1, orderId=def...
```

**Each partition has its own independent offset counter starting from 0.**

---

## Step 6 — REST Controller

Created `OrderController.java` with two endpoints:

| Endpoint | Purpose |
|----------|---------|
| `POST /api/orders/test` | Fire a hardcoded test event (no body needed) |
| `POST /api/orders` | Fire a real event with request body |

Used for manual testing during development.

---

## Step 7 — First Consumer (OrderEventConsumer)

Created `OrderEventConsumer.java` in `consumer` package.

```java
@KafkaListener(
    topics = "${topic.order}",
    groupId = "order-processor-group"
)
public void consume(ConsumerRecord<String, String> record) {
    log.info("Consumed → partition={}, offset={}, key={}, value={}",
        record.partition(), record.offset(), record.key(), record.value());
}
```

`@KafkaListener` tells Spring to continuously poll the topic. No manual polling loop needed.

`ConsumerRecord<String, String>` gives access to everything — partition, offset, key, value, timestamp.

`groupId = "order-processor-group"` — this consumer belongs to this group. Kafka tracks what offset this group has read up to independently of any other group.

### What we saw

As soon as the app started with `auto-offset-reset: earliest`, the consumer immediately read all messages that were already sitting in Kafka from earlier producer tests:

```
Consumed → partition=0, offset=0, key=abc..., value={...}
Consumed → partition=0, offset=1, key=def..., value={...}
Consumed → partition=1, offset=0, key=xyz..., value={...}
```

**Kafka retains messages even after they are consumed — they don't disappear like a queue.**

---

## Step 8 — Two Consumers in the Same Group

Added `OrderEventConsumer2.java` with the **same groupId**.

### Consumer Group: order-processor-group

| Consumer | Assigned Partition |
|----------|--------------------|
| OrderEventConsumer (ntainer#0-0-C-1) | partition=0 |
| OrderEventConsumer2 (ntainer#1-0-C-1) | partition=1 |

Kafka automatically split the 2 partitions between 2 consumers — no manual coordination code.

### What we proved

```
partition=0 messages → only Consumer-1 reads them, never Consumer-2
partition=1 messages → only Consumer-2 reads them, never Consumer-1
```

**They never read the same message. Each consumer exclusively owns a partition.**

### The Rule

```
Max parallel consumers = number of partitions
```

If we added a 3rd consumer to the same group with only 2 partitions, it would sit idle — no free partition to assign it.

---

## Step 9 — Second Consumer Group (AnalyticsConsumer)

Created `AnalyticsConsumer.java` with a **different groupId**.

```java
@KafkaListener(
    topics = "${topic.order}",
    groupId = "analytics-group"
)
```

### Consumer Groups Summary

| Group | Consumers | Purpose |
|-------|-----------|---------|
| order-processor-group | 2 (Consumer1 + Consumer2) | Process orders |
| analytics-group | 1 (AnalyticsConsumer) | Count total orders seen |

### What happened on first startup

`analytics-group` had never consumed `order-events` before. With `auto-offset-reset: earliest`, it replayed all 15 messages already in Kafka from scratch — **without firing a single curl request**:

```
Analytics → partition=0, offset=0, totalOrdersSeen=1
Analytics → partition=0, offset=1, totalOrdersSeen=2
...
Analytics → partition=1, offset=7, totalOrdersSeen=15
```

### What this proved

- `order-processor-group` had already consumed those 15 messages
- `analytics-group` got its own independent copy from offset 0
- **One group's consumption never affects another group**
- Kafka keeps messages so any new group can replay the full history

This is why Kafka is used for event sourcing, audit logs, and ML pipelines — consumers are additive, not destructive.

---

## Step 10 — Manual Offset Commit

### The Problem with Auto-Commit

With auto-commit, Kafka marks messages as "processed" every few seconds regardless of whether your code actually finished. If the app crashes mid-processing, those messages are gone.

### What we changed

```yaml
consumer:
  enable-auto-commit: false
listener:
  ack-mode: manual
```

Updated `OrderEventConsumer.java`:

```java
public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
    try {
        processOrder(record.value());
        ack.acknowledge();  // only commits AFTER successful processing
        log.info("Committed → partition={}, offset={}", ...);
    } catch (Exception e) {
        log.error("Failed → partition={}, offset={}", ...);
        // no ack.acknowledge() → Kafka will redeliver on restart
    }
}
```

### Simulated Crash Test

Made every 3rd message throw a `RuntimeException`:

```
offset=19 → Failed    (no acknowledge — stays uncommitted in Kafka)
offset=20 → Committed (processed successfully)
offset=21 → Committed (processed successfully)
offset=22 → Failed    (no acknowledge — stays uncommitted in Kafka)
```

### What this proved

Failed messages are never acknowledged. Kafka keeps them at the last committed offset. On restart, it redelivers from there — **nothing is lost**.

Meanwhile `analytics-group` consumed offset=19 and offset=22 successfully — because its offset pointer is completely independent of `order-processor-group`'s failures.

---

## How Partitions and Offsets Work Together

### Partition is per Topic

```
order-events
├── partition=0  [msg0, msg1, msg2, msg3, ...]
└── partition=1  [msg0, msg1, msg2, msg3, ...]
```

### Offset is per Partition

Each partition has its own offset counter starting from 0. There is no global offset across the topic.

```
partition=0, offset=0  ← first message in partition 0
partition=0, offset=1  ← second message in partition 0
partition=1, offset=0  ← first message in partition 1 (independent counter)
partition=1, offset=1  ← second message in partition 1
```

### Offset is per Consumer Group

Each consumer group tracks its own offset per partition independently.

```
order-processor-group → partition=0 committed at offset=6
analytics-group       → partition=0 committed at offset=6
```

Both groups read the same messages but track their own position. One group committing offset=6 has zero effect on the other group.

### Producer → Partition mapping

```
Producer sends key=orderId-AAA → hash("orderId-AAA") % 2 = partition 0
Producer sends key=orderId-BBB → hash("orderId-BBB") % 2 = partition 1
Producer sends key=orderId-AAA → hash("orderId-AAA") % 2 = partition 0  ← same partition always
```

Same key always goes to the same partition — ordering guaranteed per order.

### Consumer → Partition mapping

```
order-processor-group
├── Consumer-1 → reads partition=0 exclusively
└── Consumer-2 → reads partition=1 exclusively

analytics-group
└── AnalyticsConsumer → reads partition=0 AND partition=1 (single consumer, both partitions)
```

---

## Final Numbers

| Component | Count |
|-----------|-------|
| Producers | 1 (OrderEventProducer) |
| Topics | 3 (order-events, payment-events, inventory-events) |
| Partitions per topic | 2 |
| Consumer Groups | 2 (order-processor-group, analytics-group) |
| Consumers in order-processor-group | 2 (Consumer1 + Consumer2) |
| Consumers in analytics-group | 1 (AnalyticsConsumer) |
| Kafka Brokers | 2 (on GCP VM) |

---

## Package Structure

```
com.kafka.kafkaeventpipeline/
├── KafkaEventPipelineApplication.java
├── config/
│   └── KafkaProducerConfig.java
├── controller/
│   └── OrderController.java
├── model/
│   └── OrderEvent.java
├── producer/
│   └── OrderEventProducer.java
└── consumer/
    ├── OrderEventConsumer.java
    ├── OrderEventConsumer2.java
    └── AnalyticsConsumer.java
```

---

## Key Kafka Concepts Proved

| Concept | How we proved it |
|---------|-----------------|
| Producer → Topic | `OrderEventProducer` publishes to `order-events` |
| Key → Partition routing | Same orderId always lands on same partition |
| Offset per partition | Offset resets to 0 on each partition independently |
| Consumer Group splitting | 2 consumers split 2 partitions automatically |
| Group isolation | analytics-group replayed all messages independently |
| Message retention | Messages stayed in Kafka across multiple consumer group reads |
| Manual offset commit | Failed messages stayed uncommitted, were redeliverable |
| Failure resilience | Crash mid-processing → no data loss, analytics unaffected |