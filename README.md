# Real-Time Customer Heartbeat Monitoring System Set Up Guide

### Prerequisites

Ensure the following are installed:

- Docker

- Docker Compose

- Git

- (Optional) Python 3.11 for local development


### Clone Repository
``` bash
    git clone https://github.com/GhGuda Kafka-real-time-heartbeat-monitoring-Amalitech
    cd Kafka-Real-Time-Heartbeat-Monitoring
```

### Start the System
Build and run all services:
```bash
    docker-compose up --build
```

This will start:
- Zookeeper

- Kafka

- PostgreSQL

- Producer

- Consumer

- Prometheus

- Grafana


### Access Services

Kafka UI (if configured)
```bash
    http://localhost:8080
```

Prometheus

```bash
    http://localhost:9090
```

Grafana
``` bash
    http://localhost:3000
```
- Login:
```bash
    admin /admin
```

### Viewing Metrics

In Grafana:

1. Add Prometheus as data source:
    ``` bash
        http://prometheus:9090
    ```
2. Create dashboard panels:
    - rate(heartbeat_messages_received_total[1m])
    - rate(heartbeat_messages_inserted_total[1m])
    - heartbeat_consumer_lag


### Database Access
Connect to PostgreSQL via:

    - pgAdmin
    - Or:
        docker exec -it <postgres_container> psql -U <user> -d <db>


### Stop the System
``` bash 
    docker-compose down
```

To remove volumes (reset database):
``` bash
    docker-compose down -v
```
---





# Architecture Diagram 
```mermaid
    flowchart TB
    %% ===============================
    %% INGESTION LAYER
    %% ===============================

    subgraph Ingestion_Layer["Ingestion Layer"]
        A["Synthetic Data Generator<br/>• Generates heartbeat events<br/>• JSON payload<br/>• Configurable interval"]
        B["Kafka Producer<br/>• Publishes events<br/>• Retries + acks=all"]
    end

    %% ===============================
    %% STREAMING LAYER
    %% ===============================

    subgraph Streaming_Layer["Streaming Layer"]
        C[("Apache Kafka<br/>heartbeat-topic")]
        DLQ[("Dead Letter Topic<br/>Invalid Events")]
    end

    %% ===============================
    %% PROCESSING LAYER
    %% ===============================

    subgraph Processing_Layer["Processing Layer"]
        D["Kafka Consumer<br/>• Validation<br/>• Batch Processing<br/>• Manual Offset Commit<br/>• Prometheus Metrics"]
    end

    %% ===============================
    %% STORAGE LAYER
    %% ===============================

    subgraph Storage_Layer["Persistence Layer"]
        E[("PostgreSQL<br/>• Time-series storage<br/>• Indexed on timestamp<br/>• CHECK constraint (30–220 bpm)")]
    end

    %% ===============================
    %% OBSERVABILITY LAYER
    %% ===============================

    subgraph Observability_Layer["Observability Layer"]
        M["/metrics Endpoint (Port 8000)"]
        P[("Prometheus<br/>Time-series Metrics")]
        G["Grafana Dashboard<br/>• Messages/sec<br/>• Insert rate<br/>• Consumer lag"]
    end

    %% ===============================
    %% DATA FLOW (VERTICAL)
    %% ===============================

    A -->|Generate Events| B
    B -->|Publish Events| C
    C -->|Consume Events| D
    D -->|Valid Events| E
    D -->|Invalid Events| DLQ

    %% ===============================
    %% METRICS FLOW (RIGHT SIDE)
    %% ===============================

    D -->|Expose Metrics| M
    M --> P
    P --> G

```