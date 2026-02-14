
## Setup

### 1. Start Kafka Infrastructure

```powershell
docker-compose up -d
# or
podman-compose up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka broker (port 9092)
- Kafka UI (port 8080)

### 2. Verify Services

Check that all containers are running:

```powershell
docker ps
# or
podman ps
```

## Viewing Messages in Kafka UI

Access Kafka UI at: http://localhost:8080

## ref

https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/