# Movie Booking System — High-Level Design (HLD)

A scalable, fault-tolerant distributed system architecture for a high-concurrency movie ticket booking platform (e.g., BookMyShow). Designed to handle peak flash-crowd traffic during blockbuster releases with zero double-bookings, low latency browsing, and resilient asynchronous processing.

---

## Architecture Documents

The system architecture and technical specifications are organized into the following design documents:

| Document | Formats | Overview |
| :--- | :---: | :--- |
| **Seat Locking & Concurrency** | [PDF](Seat_Locking_Design.pdf) · [MD](Seat_Locking_Design.md) | Distributed seat locking using Redis `SETNX`, 120s TTL auto-release, sequence diagrams, and idempotency guarantees. |
| **Caching Strategy** | [PDF](Caching_Strategy.pdf) · [MD](Caching_Strategy.md) | Cache-Aside pattern, city-partitioned Redis cluster topology, short 10s TTL for live seat layouts, and stale data mitigation. |
| **Asynchronous Processing** | [PDF](Async_Workflow.pdf) · [MD](Async_Workflow.md) | Kafka event bus (`booking_events`), consumer group decoupling, exponential retries, Dead-Letter Queues (DLQ), and deduplication. |
| **Scaling & Fault Tolerance** | [PDF](Scaling_and_Fault_Tolerance.pdf) · [MD](Scaling_and_Fault_Tolerance.md) | Horizontal app scaling behind ALB, PostgreSQL city-based sharding, circuit breaker failover, and quorum write replication. |
| **Monitoring & SLOs** | [PDF](Monitoring_and_SLOs.pdf) · [MD](Monitoring_and_SLOs.md) | Core observability metrics, Prometheus & Grafana stack, and service level objectives (p95 booking latency < 2.0s). |

---

## Core System Architecture

```mermaid
flowchart TD
    Client["Client Traffic (Web / Mobile)"] --> ALB["Application Load Balancer"]
    
    subgraph Services["Stateless Service Tier"]
        BS["BookingService (Seat Locks & Orders)"]
        CS["CatalogService (Movies & Shows)"]
    end
    ALB --> Services

    subgraph Caching["In-Memory Tier"]
        Redis[("Redis Cluster<br/>• Seat Locks (TTL: 120s)<br/>• Show Cache")]
    end
    Services --> Caching

    subgraph Messaging["Event Stream (Kafka)"]
        Kafka{{"Topic: booking_events"}}
    end
    BS --> Kafka

    subgraph Consumers["Async Consumer Groups"]
        NS["NotificationService (SMS / Email)"]
        LS["LoyaltyService (Reward Points)"]
        AS["AnalyticsService (Metrics)"]
    end
    Kafka --> Consumers

    subgraph Storage["Persistent Storage Tier"]
        DB[("PostgreSQL (Sharded by City)<br/>Primary Writes + Read Replicas")]
    end
    Services --> Storage

    subgraph Integrations["External Integrations"]
        CB["Circuit Breaker"] --> PG["Payment Gateway"]
    end
    BS --> Integrations
```

---

## Key Design Decisions

1. **Distributed Seat Locking:** Temporary seat holds use Redis atomic `SET seat:{show_id}:{seat_id} {user_id} NX EX 120`. This ensures sub-millisecond lock acquisition and eliminates heavy database row contention. Expired holds auto-release at 120 seconds.
2. **Two-Tiered Caching:** Show details are cached for 300 seconds using the Cache-Aside pattern. Seat availability maps use a short 10-second TTL to balance database read protection with UI freshness, while the atomic Redis lock acts as the single source of truth.
3. **Decoupled Checkout Path:** Post-booking operations (SMS tickets, loyalty points, analytics logs) run asynchronously through Kafka topic `booking_events`. Consumers process messages independently with exponential retries and dead-letter queues (`booking_events_dlq`).
4. **City-Based Database Sharding:** PostgreSQL is horizontally sharded by `city_id`. Because movie discovery and ticket reservations are geographically localized, transactional locks and queries execute within single shards without cross-shard joins.
5. **Circuit Breakers & Observability:** Outbound calls to payment processors are wrapped with circuit breakers that fail over to backup providers during third-party outages. Prometheus and Grafana monitor latency with a strict SLO of **p95 booking completion in < 2.0 seconds**.
