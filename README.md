# FluxGuard

**FluxGuard** is a Go-based real-time event processing and anomaly detection engine built around **Apache Kafka**.

It accepts events through an HTTP API, streams them through Kafka, maintains per-key sliding windows, evaluates configurable anomaly rules, and publishes detected anomalies back to Kafka.

The project demonstrates practical concepts in:

- Go backend development
- Event-driven architecture
- Apache Kafka
- Streaming data processing
- Sliding-window analytics
- Concurrency and synchronization
- Docker
- Kubernetes
- GitHub Actions CI/CD
- GitHub Container Registry

---

## Architecture

```text
                         HTTP Client
                              |
                              | POST /events
                              v
                       +----------------+
                       |  FluxGuard API |
                       |      Go        |
                       +-------+--------+
                               |
                               | publish event
                               v
                        +-------------+
                        |    Kafka    |
                        |             |
                        | fluxguard-  |
                        | events      |
                        +------+------+ 
                               |
                               | consume
                               v
                     +----------------------+
                     | FluxGuard Processor  |
                     |         Go           |
                     +----------+-----------+
                                |
                    +-----------+-----------+
                    |                       |
                    v                       v
             Sliding Window          Rule Engine
             per event key           - Count Rule
                                     - Total Value Rule
                    |                       |
                    +-----------+-----------+
                                |
                              anomaly
                                |
                                v
                        +---------------+
                        |     Kafka     |
                        | fluxguard-    |
                        | anomalies     |
                        +---------------+
```

---

## How It Works

### 1. Event Ingestion

The API exposes:

```text
POST /events
```

An event contains:

```json
{
  "id": "evt-123",
  "type": "transaction",
  "source": "payment-service",
  "key": "user-42",
  "value": 1500,
  "timestamp": "2026-09-28T10:00:00Z"
}
```

The API validates the event and publishes it to Kafka.

The Kafka message key is derived from the event's `key` field, allowing events for the same logical entity to be routed consistently.

---

### 2. Kafka Streaming

FluxGuard uses Apache Kafka as the event transport layer.

Two application topics are used:

```text
fluxguard-events
fluxguard-anomalies
```

The event topic carries incoming events to the processor.

The anomaly topic receives detected anomaly records.

---

### 3. Sliding-Window Processing

The processor maintains an in-memory sliding window for each event key.

The current window duration is:

```text
10 seconds
```

For every incoming event, expired events are removed and the current event is added to the corresponding key's window.

Example:

```text
Key: user-42

10-second window
┌─────────────────────────────────┐
│ evt-1  evt-2  evt-3  evt-4     │
└─────────────────────────────────┘
                  ^
             new event
```

This allows FluxGuard to reason about recent activity rather than processing each event independently.

---

## Anomaly Detection

FluxGuard currently provides two rules.

### High Event Count

Triggers when at least **5 events** for the same key exist inside the active window.

```text
count(events) >= 5
```

Rule name:

```text
high_event_count
```

### High Total Value

Triggers when the sum of event values inside the active window reaches **5000**.

```text
sum(event.value) >= 5000
```

Rule name:

```text
high_total_value
```

A single event can satisfy multiple rules.

An emitted anomaly contains:

```json
{
  "event_id": "evt-5",
  "key": "user-42",
  "rules": [
    "high_event_count",
    "high_total_value"
  ],
  "detected_at": "2026-09-28T10:00:05Z"
}
```

---

## Project Structure

```text
FluxGuard/
├── .github/
│   └── workflows/
│       └── cicd.yml
│
├── cmd/
│   ├── api/
│   │   └── main.go
│   └── processor/
│       └── main.go
│
├── internal/
│   ├── api.go
│   ├── config.go
│   ├── detector.go
│   ├── kafka.go
│   ├── model.go
│   ├── processor.go
│   ├── rules.go
│   └── window.go
│
├── k8s/
│   ├── api.yaml
│   ├── configmap.yaml
│   ├── kafka.yaml
│   ├── processor.yaml
│   └── topics.yaml
│
├── tests/
│   ├── detector_test.go
│   └── window_test.go
│
├── compose.yaml
├── Dockerfile
├── go.mod
├── go.sum
└── .gitignore
```

---

## Components

### API

Located in:

```text
cmd/api
internal/api.go
```

Responsibilities:

- Accept incoming HTTP events
- Validate request payloads
- Publish events to Kafka
- Expose a health endpoint

Endpoints:

```text
GET  /health
POST /events
```

---

### Processor

Located in:

```text
cmd/processor
internal/processor.go
```

Responsibilities:

- Consume events from Kafka
- Maintain per-key sliding windows
- Evaluate anomaly rules
- Publish detected anomalies
- Commit successfully processed Kafka messages

The processor uses the Kafka consumer group:

```text
fluxguard-processors
```

This provides a foundation for distributing Kafka partitions across multiple processor instances.

---

### Window Manager

Located in:

```text
internal/window.go
```

The window manager stores events grouped by key:

```text
map[string][]Event
```

It uses a `sync.RWMutex` to protect shared state.

The current implementation maintains a **10-second event-time-based window** by filtering events using the incoming event timestamp.

---

### Rule Engine

Located in:

```text
internal/rules.go
```

The detector uses a small rule abstraction:

```go
type Rule interface {
    Name() string
    Evaluate(events []Event) bool
}
```

This allows additional anomaly rules to be added without changing the processor's core detection flow.

---

## Docker

FluxGuard uses a multi-stage Docker build.

The build stage uses:

```text
golang:1.26
```

The runtime images use:

```text
gcr.io/distroless/static-debian12:nonroot
```

Two Docker targets are provided:

```text
api
processor
```

Build locally with:

```bash
docker build --target api -t fluxguard-api .
docker build --target processor -t fluxguard-processor .
```

---

## Running with Docker Compose

The recommended local setup uses Docker Compose:

```bash
docker compose up --build
```

The Compose environment runs:

```text
Kafka
  ↓
Kafka topic initialization
  ↓
FluxGuard API
  ↓
FluxGuard Processor
```

The Kafka initialization service explicitly creates:

```text
fluxguard-events
fluxguard-anomalies
```

before the application services start.

This makes the environment deterministic for both local development and CI/CD deployment testing.

---

## Sending an Event

Once the stack is running:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt-1",
    "type": "transaction",
    "source": "payment-service",
    "key": "user-42",
    "value": 1000,
    "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"
  }'
```

The API should respond with:

```text
event accepted
```

To trigger the event-count rule, send at least five events for the same key within the 10-second window.

---

## Configuration

FluxGuard is configured through environment variables.

### API

```text
FLUXGUARD_HTTP_PORT
FLUXGUARD_KAFKA_BROKER
FLUXGUARD_KAFKA_TOPIC
FLUXGUARD_KAFKA_ANOMALY_TOPIC
```

Defaults include:

```text
HTTP port:        8080
Kafka broker:     localhost:9092
Event topic:      fluxguard-events
Anomaly topic:    fluxguard-anomalies
```

The Docker Compose deployment overrides the Kafka broker with:

```text
kafka:9092
```

because the application runs inside the Compose network.

---

## Kubernetes

FluxGuard also includes Kubernetes manifests under:

```text
k8s/
```

The manifests define:

- FluxGuard API Deployment
- FluxGuard API Service
- FluxGuard Processor Deployment
- Kafka deployment through Strimzi resources
- Kafka topics
- Application configuration

The Kubernetes setup uses:

```text
Apache Kafka
+
Strimzi
+
FluxGuard API
+
FluxGuard Processor
```

The Kubernetes manifests are separate from the GitHub Actions Compose-based deployment verification.

---

## Testing

Run the test suite locally with:

```bash
go test ./...
```

Current tests cover:

### Detector Tests

- High event-count anomaly detection
- High total-value anomaly detection
- Normal activity without an anomaly

### Window Tests

- Expired event removal
- Separation of events by key

---

## CI/CD

FluxGuard uses a unified GitHub Actions workflow:

```text
.github/workflows/cicd.yml
```

The workflow is structured as:

```text
                    Git push
                       |
                       v
                     Test
                 go test ./...
                       |
                       v
                    Build
             Docker API + Processor
                       |
                       v
                   Publish
              GitHub Container
                 Registry
                       |
                       v
                    Deploy
             Docker Compose stack
                       |
                       v
                 Smoke Testing
             API + Kafka + Processor
                       |
                       v
             Verify Anomaly Detection
                       |
                       v
                    Cleanup
```

### Continuous Integration

For pushes and pull requests, GitHub Actions runs:

```bash
go test ./...
```

and builds both Docker targets.

Pull requests do not publish images or perform deployments.

---

### Continuous Delivery

For pushes to `main`, the pipeline builds immutable images tagged with the commit SHA:

```text
ghcr.io/abhianand-1011/fluxguard-api:<commit-sha>
ghcr.io/abhianand-1011/fluxguard-processor:<commit-sha>
```

The images are published to GitHub Container Registry.

---

### Continuous Deployment

After publishing:

1. GitHub Actions pulls the exact images generated for the commit.
2. Docker Compose starts Kafka, topic initialization, API, and Processor.
3. The API health endpoint is checked.
4. Smoke-test events are submitted through the API.
5. The pipeline verifies that the Processor detects the expected anomaly.
6. The temporary deployment environment is cleaned up.

The deployment therefore verifies the complete path:

```text
source code
    ↓
tests
    ↓
Docker images
    ↓
container registry
    ↓
running Kafka + application
    ↓
real HTTP events
    ↓
stream processing
    ↓
anomaly detection
```

---

## CI/CD Workflow Dependencies

The workflow is intentionally ordered so deployment cannot occur before the earlier stages succeed:

```text
test
  ↓
build
  ↓
publish
  ↓
deploy
```

Pull requests run the test and build stages without publishing or deploying.

Pushes to `main` run the complete pipeline.

---

## Technical Highlights

### Event-Driven Architecture

Kafka decouples event ingestion from downstream processing:

```text
HTTP API → Kafka → Processor → Anomaly Topic
```

### Key-Based Kafka Partitioning

Events are published using the event key as the Kafka message key, allowing Kafka's hash-based partitioning to keep events for the same key on the same partition under normal partition assignment.

### Concurrent State Protection

The sliding-window state is protected with:

```go
sync.RWMutex
```

allowing concurrent reads and synchronized writes.

### Consumer Groups

The processor uses the Kafka consumer group:

```text
fluxguard-processors
```

which provides a foundation for distributing Kafka partitions across multiple processor instances.

### At-Least-Once Processing Behavior

Kafka messages are committed after processing and anomaly publication.

A failure occurring after downstream publication but before the Kafka offset commit can therefore result in the message being processed again.

### Graceful Shutdown

Both the API and Processor respond to:

```text
SIGINT
SIGTERM
```

and perform controlled shutdown.

---

## Limitations

FluxGuard is a focused distributed-systems project rather than a production-hardened streaming platform.

Current limitations include:

- Sliding-window state is held in process memory.
- Processor restarts therefore lose its active window state.
- The window is maintained using incoming event timestamps and does not implement watermarking or event-time reconciliation.
- Inactive keys are not independently expired without subsequent events.
- A rule that remains satisfied can produce anomaly records on subsequent events while the condition remains true.
- Kafka deployment examples use a single broker for simplicity.
- The Docker Compose environment is designed for local development and temporary CI/CD verification rather than persistent production hosting.
- The Kubernetes manifests use single-replica application/Kafka configurations for a lightweight deployment model.

These constraints keep the project focused while leaving clear paths for future distributed-systems improvements.

---

## Future Improvements

Possible extensions include:

- Persistent or distributed window state
- Kafka-based state recovery
- Exactly-once or idempotent anomaly handling
- Watermarks and stronger event-time processing
- More configurable rule definitions
- Dynamic rule loading
- Dead-letter handling for invalid events
- Prometheus metrics
- OpenTelemetry tracing
- Horizontal processor scaling
- Multi-broker Kafka deployment
- Automated vulnerability scanning
- Deployment rollback support

---

## Tech Stack

| Area | Technology |
|---|---|
| Language | Go |
| Streaming | Apache Kafka |
| Kafka Client | kafka-go |
| API | Go `net/http` |
| Containers | Docker |
| Local Orchestration | Docker Compose |
| Kubernetes | Kubernetes + Strimzi |
| CI/CD | GitHub Actions |
| Container Registry | GitHub Container Registry |
| Testing | Go testing package |

---

## What This Project Demonstrates

FluxGuard brings together several backend and distributed-systems concepts in one project:

```text
Go
├── HTTP servers
├── concurrency
├── synchronization
├── graceful shutdown
└── modular design

Kafka
├── event streaming
├── partitions
├── message keys
├── consumer groups
└── offset commits

Streaming Processing
├── sliding windows
├── per-key state
└── rule-based anomaly detection

DevOps
├── Docker
├── Docker Compose
├── Kubernetes
├── GitHub Actions
└── GitHub Container Registry
```

---

## License

This project is intended as a systems and backend engineering project for learning and experimentation.
